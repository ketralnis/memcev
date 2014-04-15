#include <sys/socket.h>
#include <fcntl.h>

#include <Python.h>
#include <ev.h>

#include "_memcevmodule.h"

static PyObject* _MemcevClient_start(_MemcevClient *self, PyObject *unused) {
    /* this is the function called in its own Thread */

    // we don't need to hold the GIL while waiting in the event loop
    Py_BEGIN_ALLOW_THREADS;

    ev_run(self->loop, 0);

    Py_END_ALLOW_THREADS;

    // all done! someone must have terminated us with ev_break
    Py_RETURN_NONE;
}

static PyObject* _MemcevClient_stop(_MemcevClient *self, PyObject *unused) {
    // signal to the event loop that it should stop what it's doing. This will
    // cause start() to return, leaving self.requests potentially full of work
    // to do
    ev_break(self->loop, EVBREAK_ALL);

    Py_RETURN_NONE;
}


static PyObject* _MemcevClient_notify(_MemcevClient *self, PyObject *unused) {
    /*
    This is the MemcevClient.notify() Python method to let the event loop know that
    a new entry has been added on the self.requests queue. All we do is pass
    this information onto the event loop, who will trigger notify_event_loop to
    do the work in that thread.
    */

    // the docs claim that this doesn't block, but since it's thread-safe there
    // is probably a mutex in there that I'd rather not block Python with if we
    // don't have to
    Py_BEGIN_ALLOW_THREADS;
    ev_async_send(self->loop, &self->async_watcher);
    Py_END_ALLOW_THREADS;

    Py_RETURN_NONE;
}

// static ev_connection* make_connection(char* host, int port) {
//     ev_connection* ret = malloc(sizeof(ev_connection));

//     if(ret == NULL) {

//         return NULL;
//     }

//     int sock = socket(PF_INET, SOCK_STREAM, 0);

//     if(sock == -1) {
//         /* TODO error */
//     }

//     ret.fd = sock;
//     ret.state = connecting;

//     /* set it non-blocking */
//     if(-1 == fcntl(sock, F_SETFL, O_NONBLOCK | fcntl(sock, F_GETFL))) {
//         /* TODO error */
//     }

//     // if (-1 == connect(sock, (struct sockaddr *)&daemon, len)) {
//     //     // TODO error

//     // }

//     return ret;
// }

static void notify_event_loop(struct ev_loop *loop, ev_async *watcher, int revents) {
    /*
    called by MemcevClient.notify() to let us know that a new request has been
    added to the self.requests queue
    */

    // we're not called while holding the GIL, so we have to grab it to get
    // access to Python methods
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    _MemcevClient *self = (_MemcevClient*)ev_userdata(loop);

    PyObject* result = PyObject_CallMethod((PyObject*)self, "handle_work", NULL);

    if(result == NULL) {
        // if an exception occurred, there's not really much we can do since
        // we're in our own thread and there's nobody to raise it to. So
        // hopefully handle_work() handles all of the errors that aren't
        // programming errors
        PyErr_Print();
    }

    Py_XDECREF(result);

    // don't need the GIL anymore after we've handled all of the work
    PyGILState_Release(gstate);
}

static int _MemcevClient_init(_MemcevClient *self, PyObject *args, PyObject *kwargs) {
    int ret = 0;

    static char *kwdlist[] = {NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "",
                                     kwdlist)) {
        // we take no arguments
        return -1;
    }

    // we have to initialise this here instead of letting the eventloop thread
    // do it, because we need a handle to it and to be able to promise that it
    // can be called before we can promise that the event loop has initialised
    // it
    self->loop = ev_loop_new(EVFLAG_AUTO);

    if(!self->loop) {
        PyErr_SetString(PyExc_RuntimeError, "Unable to create event loop");
        ret = -1;
        goto cleanup;
    }

    /* TODO catch errors here and pass them up as well */
    ev_async_init(&self->async_watcher, notify_event_loop);
    ev_async_start(self->loop, &self->async_watcher);

    /* give that watcher access to our struct */
    ev_set_userdata(self->loop, self);

cleanup:

    return ret;
}

static void _MemcevClient_dealloc(_MemcevClient* self) {
    /*
    this isn't called until the event loop finishes running, so it should be
    safe to clean up everything including the libev objects
    */

    Py_XDECREF(self->host);
    Py_XDECREF(self->connections);
    Py_XDECREF(self->requests);
    Py_XDECREF(self->thread);

    if(self->loop != NULL) {
        ev_loop_destroy(self->loop);
    }
    /* the async_watcher has no cleanup method, so I think it's safe to assume
       that it has no state after it's not used? */

    self->ob_type->tp_free((PyObject*)self);
}


PyMODINIT_FUNC init_memcev(void) {
    /* initialise the module */

    PyObject* module;

    /* initialise the EventLoop type */

    /* have to do this here because some C compilers have issues with static
       references between modules. we can take this out when we make our own */
    _MemcevClientType.tp_new = PyType_GenericNew;

    if (PyType_Ready(&_MemcevClientType) < 0) {
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
    Py_INCREF(&_MemcevClientType);
    PyModule_AddObject(module, "_MemcevClient", (PyObject *)&_MemcevClientType);
}
