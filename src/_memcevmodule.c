#include <Python.h>
#include <ev.h>

#include "_memcevmodule.h"

static PyObject* _MemcevEventLoop_start(_MemcevEventLoop *self, PyObject *unused) {
    /* this is the function called in its own Thread */

    // we need to make sure that we can keep a handle on the outer object. n.b.
    // that this makes a circular reference, so we have to make sure to clean
    // ourself up
    Py_INCREF(self);

    /*
    we don't need to hold the GIL while in the event loop, any anyone that wakes
    us up will acquire it themselves
    */

    Py_BEGIN_ALLOW_THREADS;

    printf("starting ev_run\n");

    /*
    normally you call ev_run and live in it forever, but he exits when he runs
    out of watchers, which we haven't set up yet (and the async_watcher doesn't
    count). fortunately there doesn't look  to be a performance cost to this
    model
    */

    ev_run(self->loop,0);

    printf("returning from ev_run\n");

    Py_END_ALLOW_THREADS;

    /* to match the INCREF above */
    Py_DECREF(self);

    /* all done! */
    Py_RETURN_NONE;
}


static PyObject* _MemcevEventLoop_notify(_MemcevEventLoop *self, PyObject *unused) {
    /*
    This is the EventLoop.notify() Python method to let the event loop know that
    a new entry has been added on the self.requests queue. All he does is pass
    this information onto the event loop, who will trigger notify_event_loop to
    do the work.
    */

    /* the docs claim that this doesn't block, but since it's thread-safe there
       is probably a mutex in there that I'd rather not block Python with if we don't
       have to */
    Py_BEGIN_ALLOW_THREADS;
    ev_async_send(self->loop, &self->async_watcher);
    Py_END_ALLOW_THREADS;

    Py_RETURN_NONE;
}

// static int make_connection(char* host, int port) {
//     int sock = socket(PF_INET, SOCK_STREAM, 0);

//     if(sock == -1) {
//         /* TODO error */
//     }

//     /* set it non-blocking */
//     if(-1 == fcntl(sock, F_SETFL, O_NONBLOCK | fcntl(sock, F_GETFL))) {
//         /* TODO error */
//     }

//     return sock;
// }

static void notify_event_loop(struct ev_loop *loop, ev_async *watcher, int revents) {
    /*
    called by EventLoop.notify() to let us know that a new request has been
    added to the self.requests queue
    */

    // we're not called while holding the GIL, so we have to grab it to get
    // access to self->requests
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    _MemcevEventLoop *self = (_MemcevEventLoop*)ev_userdata(loop);

    while(get_and_handle_work(self)) {
        // keep doing this as long as we think there might be more work to do
    }

    // don't need the GIL anymore
    PyGILState_Release(gstate);
}

static int get_and_handle_work(_MemcevEventLoop *self) {
    // called in the event loop thread. we don't know that there is work to do
    // yet

    PyObject* work_result = PyObject_CallMethod(self->requests, "get_nowait", NULL);

    if(work_result == NULL) {
        // we failed to pull work out of the queue. this might be just fine if
        // it's because two async events were coallesed, so we need to see if
        // it's the Empty exception first
        if(PyErr_ExceptionMatches(self->empty_exception)) {
            // then there's no problem, just move on
            printf("I got some fake work\n");
            PyErr_Clear();
            return 0;
        } else {
            // what the heck do we do here? there's nobody around to respond to
            // it. all we did was check to see if it's empty, so I guess we can
            // just print that and move on, but it's unlikely to work next time
            // either
            PyErr_Print();
            goto cleanup;
        }
    }

    printf("Got work item: ");
    PyObject_Print(work_result, stdout, 0);
    printf("\n");

    // otherwise work_result is a tuple describing the work to do
    if(!PyTuple_Check(work_result)
        || PyTuple_GET_SIZE(work_result) < 1
        || !PyString_Check(PyTuple_GET_ITEM(work_result, 0))) {

        // of course since nobody can catch our exception, the best we can do is
        // print it
        PyErr_SetString(PyExc_TypeError, "_Memcev work items must be tuples");
        PyErr_Print();
        goto cleanup;
    };


    PyObject* tag_object = PyTuple_GET_ITEM(work_result, 0);
    char* tag = PyString_AS_STRING(tag_object);

cleanup:

    Py_XDECREF(work_result);

    return 1;
}

static int _MemcevEventLoop_init(_MemcevEventLoop *self, PyObject *args, PyObject *kwargs) {
    unsigned int size;
    char* host;
    int port;
    PyObject* queue_module = NULL;

    int ret = 0;

    static char *kwdlist[] = {"host", "port", "size", "requests", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "siiO",
                                     kwdlist,
                                     &host,
                                     &port,
                                     &size,
                                     &self->requests
                                     )) {
        return -1;
    }

    Py_INCREF(self->requests);

    queue_module = PyImport_ImportModule("Queue");
    if(queue_module == NULL) {
        ret = -1;
        goto cleanup;
    }

    // we need a handle on this because we need to compare against it in
    // notify(). Since that is called in its own thread, I'd rather know if we
    // can't import it before that or nobody will be around to catch the
    // ImportError
    self->empty_exception = PyObject_GetAttrString(queue_module, "Empty");
    if(self->empty_exception == NULL) {
        ret = -1;
        goto cleanup;
    }

    /*
    we have to initialise this here instead of letting the eventloop thread do
    it, because we need a handle to it and to be able to promise that it can be
    called before we can promise that the event loop has initialised it
    */
    self->loop = ev_loop_new(EVFLAG_AUTO);

    if(!self->loop) {
        PyErr_SetString(PyExc_RuntimeError, "Unable to create event loop");
        ret = -1;
        goto cleanup;
    }

    /* TODO catch errors here and pass them up */
    ev_async_init(&self->async_watcher, notify_event_loop);
    ev_async_start(self->loop, &self->async_watcher);

    /* give that watcher access to our struct */
    ev_set_userdata(self->loop, self);

cleanup:

    Py_XDECREF(queue_module);

    return ret;
}

static void _MemcevEventLoop_dealloc(_MemcevEventLoop* self) {
    /*
    this isn't called until the event loop finishes running, so it should be
    safe to clean up everything including the libev objects
    */

    Py_XDECREF(self->requests);
    Py_XDECREF(self->empty_exception);
    self->ob_type->tp_free((PyObject*)self);

    if(self->loop != NULL) {
        ev_loop_destroy(self->loop);
    }
    /* the async_watcher has no cleanup method, so I think it's safe to assume
       that it has no state after it's not used? */
}


PyMODINIT_FUNC init_memcev(void) {
    /* initialise the module */

    PyObject* module;

    /* initialise the EventLoop type */

    /* have to do this here because some C compilers have issues with static
       references between modules. we can take this out when we make our own */
    _MemcevEventLoopType.tp_new = PyType_GenericNew;

    if (PyType_Ready(&_MemcevEventLoopType) < 0) {
        /* exception raised in preparing */
        return;
    }

    module = Py_InitModule3("_memcev",
        NULL, /* no functions of our own */
        "C module that implements the memcev event loop");

    if (module == NULL) {
        /* exception raised in preparing */
        return;
    }

    /* make it visible */
    Py_INCREF(&_MemcevEventLoopType);
    PyModule_AddObject(module, "EventLoop", (PyObject *)&_MemcevEventLoopType);
}
