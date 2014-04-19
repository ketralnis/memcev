#include <sys/socket.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>

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

static PyObject* _MemcevClient_notify(_MemcevClient *self, PyObject *cb) {
    // This is the MemcevClient.notify() Python method to let the event loop
    // know that a new entry has been added on the self.requests queue. All we
    // do is pass this information onto the event loop, who will trigger
    // notify_event_loop (through the async_watcher) to do the work in that
    // thread.

    // the docs claim that this doesn't block, but since it's thread-safe there
    // is probably a mutex in there that I'd rather not block Python with if we
    // don't have to
    Py_BEGIN_ALLOW_THREADS;
    // TODO do we have to check for an error here?
    ev_async_send(self->loop, &self->async_watcher);
    Py_END_ALLOW_THREADS;

    Py_RETURN_NONE;
}

static void free_connection_capsule(PyObject *capsule) {
    ev_connection* connection = PyCapsule_GetPointer(capsule, NULL);
    if(connection == NULL) {
        return;
    }

    // TODO lots of stuff to do here; remove any watchers? close connection? Is
    // it safe to close?

    free(connection);
}

static void connect_cb(struct ev_loop* loop, ev_io *w, int revents) {
    if(!(EV_WRITE & revents)) {
        return;
    }

    // clean all of this up first and extract just the connection and callback
    ev_io_stop(loop, w);
    connect_request* request = w->data;
    ev_connection* connection = request->connection;
    PyObject* callback = request->callback;
    free(request);
    free(w);

    int so_error;
    socklen_t len = sizeof so_error;
    int sockopt_result = getsockopt(connection->fd, SOL_SOCKET,
                                    SO_ERROR, &so_error, &len);

    if(sockopt_result != -1 && so_error == 0) {
        // success!
        connection->state = connected;
    } else {
        connection->state = error;
        close(connection->fd);
        if(sockopt_result == -1) {
            connection->error = strerror(errno);
        } else {
            connection->error = strerror(so_error);
        }
    }

    // now grab the GIL so we can tell the client all about it
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    // call the callback with either an error tuple or a success tuple
    // containing PyCapsule containing a connection*

    PyObject* status_tuple = NULL;
    PyObject* none_result = NULL;
    PyObject* capsule = NULL;

    if(connection->state == error) {
        status_tuple = Py_BuildValue("((ss))", "error", connection->error);
        free(connection);
        if(status_tuple == NULL) {
            goto sock_cleanup;
        }
    } else {
        capsule = PyCapsule_New(connection, NULL, free_connection_capsule);
        if(capsule == NULL) {
            goto sock_cleanup;
        }

        status_tuple = Py_BuildValue("((ss))", "connected", capsule);
        if(status_tuple == NULL) {
            goto sock_cleanup;
        }
    }

    none_result = PyObject_CallObject(callback, status_tuple);
    if(none_result == NULL) {
        // if he didn't exit successfully, we don't really know if he did his
        // job to add the capsule to the connections queue, so we don't know
        // whether we should close the socket. since we're this far, we know
        // that the object has been created successfully so we'll rely on
        // refcounting to close it now
    }

    goto cleanup;

sock_cleanup:
    // an error occurred that means we need to clean up the socket first
    close(connection->fd);
    free(connection);

cleanup:
    Py_DECREF(ev_userdata(loop));
    Py_DECREF(callback);
    Py_XDECREF(status_tuple);
    Py_XDECREF(capsule);
    Py_XDECREF(none_result);

    if(PyErr_Occurred()) {
        // not much else I can do here, we're on the event loop's thread
        PyErr_Print();
    }

    PyGILState_Release(gstate);
}

static PyObject* _MemcevClient__connect(_MemcevClient *self, PyObject *cb) {
    PyObject* ret = NULL;

    Py_INCREF(cb);

    ev_connection* connection = NULL;

    ev_io* connect_watcher = NULL;
    connect_request* request = NULL;

    char* hostname = PyString_AsString((PyObject*)self->host);

    if(hostname == NULL) {
        // somebody messed with my hostname
        return NULL;
    };

    connect_watcher = malloc(sizeof(ev_io));
    if(connect_watcher == NULL) {
        ret = PyErr_NoMemory();
        goto error_lbl;
    }

    request = malloc(sizeof(connect_request));
    if(request == NULL) {
        ret = PyErr_NoMemory();
        goto error_lbl;
    }

    // he's going to do a blocking network call to resolve DNS
    Py_BEGIN_ALLOW_THREADS;

    connection = make_connection(hostname, self->port);

    Py_END_ALLOW_THREADS;

    if(connection == NULL) {
        ret = PyErr_NoMemory();
        goto error_lbl;
    }

    if(connection->state == error) {
        // since we're connecting in a non-blocking way, these errors can only
        // be errors in DNS resolution or allocation failures. we don't find out
        // about any others until later
        PyErr_SetString(PyExc_RuntimeError, connection->error);
        goto error_lbl;
    }

    connect_watcher->data = request;
    request->connection = connection;
    request->callback = cb;

    // that request now has a reference to us
    Py_INCREF(self);

    ev_io_init(connect_watcher, connect_cb, connection->fd, EV_WRITE);
    ev_io_start(self->loop, connect_watcher);

    Py_RETURN_NONE;

error_lbl:
    if(connection != NULL) {
        free(connection);
    }
    if(connect_watcher != NULL) {
        free(connect_watcher);
    }
    if(request != NULL) {
        free(request);
    }
    Py_DECREF(cb);

    return ret;
}

static ev_connection* make_connection(char* host, int port) {
    ev_connection* ret = malloc(sizeof(ev_connection));

    if(ret == NULL) {
        return NULL;
    }

    int sock = socket(PF_INET, SOCK_STREAM, 0);

    if(sock == -1) {
        ret->state = error;
        ret->error = strerror(errno);

        goto cleanup;
    }

    ret->fd = sock;
    ret->state = connecting;

    /* set it non-blocking */
    if(-1 == fcntl(sock, F_SETFL, O_NONBLOCK | fcntl(sock, F_GETFL))) {
        ret->state = error;
        ret->error = strerror(errno);

        goto cleanup;
    }

    // libev doesn't have an async DNS implementation like libevent, and I don't
    // really want to pull in libadns as a dependency. but we won't be doing
    // this very often anyway
    struct hostent* he = gethostbyname(host);
    if(he == NULL) {
        ret->state = error;
        ret->error = (char*)hstrerror(h_errno);
        goto cleanup;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port); // I really wish the OS did the byte-swapping on the port

    memcpy(&server.sin_addr, he->h_addr_list[0], he->h_length);

    int connect_status = connect(sock, (struct sockaddr *)&server, sizeof(server));

    if(connect_status == 0) {
        ret->state = error;
        ret->error = "unexpected success";
    } else if(errno != EINPROGRESS) {
        // anything else is wrong
        ret->state = error;
        ret->error = strerror(errno);
    }

cleanup:
    if(ret->state == error && sock != -1) {
        close(sock);
    }

    // hostents are cleaned up/reused by the system

    return ret;
}

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
