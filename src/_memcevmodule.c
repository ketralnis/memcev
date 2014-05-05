#include <sys/socket.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>

#include <Python.h>
#include <ev.h>

#include "_memcevmodule.h"

static PyObject* _MemcevClient_start(_MemcevClient *self, PyObject *unused) {
    // this is the function called in its own Thread

    Py_INCREF(self);

    // we don't need to hold the GIL while waiting in the event loop as long as
    // the watchers obtain it
    Py_BEGIN_ALLOW_THREADS;

    ev_run(self->loop, 0);

    Py_END_ALLOW_THREADS;

    Py_DECREF(self);

    // all done! someone must have terminated us with ev_break
    Py_RETURN_NONE;
}

static PyObject* _MemcevClient_stop(_MemcevClient *self, PyObject *unused) {
    // called on the event loop thread, signal to the event loop that it should
    // stop what it's doing. This will cause start() to return, leaving
    // self.requests potentially full of work to do. This will cause any active
    // clients to hang, but this should only be called on dealloc anyway
    ev_break(self->loop, EVBREAK_ALL);

    Py_RETURN_NONE;
}

static void notify_event_loop(struct ev_loop *loop, ev_async *watcher, int revents) {
    // triggered on the the event loop thread by a call to MemcevClient.notify()
    // to let us know that a new request has been added to the self.requests
    // queue

    // we're not called while holding the GIL, so we have to grab it to get
    // access to Python methods
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    _MemcevClient *self = (_MemcevClient*)ev_userdata(loop);
    Py_INCREF(self);

    PyObject* result = PyObject_CallMethod((PyObject*)self, "_handle_work", NULL);

    if(result == NULL) {
        // if an exception occurred, there's not really much we can do since
        // we're in our own thread and there's nobody to raise it to. So
        // hopefully handle_work() handles all of the errors that aren't
        // programming errors
        PyErr_Print();
    }

    Py_XDECREF(result);
    Py_DECREF(self);

    // don't need the GIL anymore after we've handled all of the work
    PyGILState_Release(gstate);
}

static PyObject* _MemcevClient_notify(_MemcevClient *self, PyObject *cb) {
    // This is the MemcevClient.notify() Python method to let the event loop
    // know that a new entry has been added on the self.requests queue. All we
    // do is pass this information onto the event loop, who will trigger
    // notify_event_loop (through the async_watcher) to do the work in that
    // thread. It's safe to have multiple outstanding notifies going on

    // the docs claim that ev_async_send doesn't block, but since it's thread-
    // safe there is probably a mutex in there that I'd rather not block
    // Python with if we don't have to
    Py_BEGIN_ALLOW_THREADS;
    ev_async_send(self->loop, &self->async_watcher);
    Py_END_ALLOW_THREADS;

    Py_RETURN_NONE;
}

static void getset_request_cb(struct ev_loop* loop, ev_io *watcher, int revents) {
    // libev will call us here when we're ready to send the request, and again
    // when we're ready to receive new data. we funnel that off to the parsing
    // callbacks passed to us from Python repeatedly until he says he's done

    getset_request* req = (getset_request*)watcher->data;

    ev_connection* connection = NULL;

    // intermediate results that we use
    PyObject* newacc = NULL;
    PyObject* parse_response = NULL;
    PyObject* done_bool = NULL;
    PyObject* done_none_result = NULL;

    // used in the error: label
    PyObject* error_none_result = NULL;
    PyObject* ptype = NULL;
    PyObject* pvalue = NULL;
    PyObject* ptraceback = NULL;

    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    connection = PyCapsule_GetPointer(req->connection, "connection");
    if(connection == NULL) {
        goto error;
    }

    if(EV_WRITE & revents) {
        if(req->state == getset_not_started) {
            char* request_string = PyString_AsString(req->body);
            if(request_string == NULL) {
                goto error;
            }

            size_t sent_size = send(connection->fd, request_string,
                                    PyString_Size(req->body), 0);

            if(sent_size == -1) {
                // have to build our own error
                PyErr_SetFromErrno(PyExc_IOError);
                goto error;
            }

            req->state = getset_awaiting_response;

            // rejigger the watcher to catch READ events now
            ev_io_stop(loop, watcher);
            ev_io_set(watcher, connection->fd, EV_READ);
            ev_io_start(loop, watcher);
        }
    } else if(EV_READ & revents) {
        if(req->state != getset_awaiting_response) {
            PyErr_SetString(PyExc_RuntimeError, "bad i/o state");
            goto error;
        }

        const size_t buffer_size = 1024;
        char buffer[buffer_size];

        size_t received_size = recv(connection->fd, buffer, buffer_size, 0);

        if(received_size == -1) {
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }

        parse_response = PyObject_CallFunction(req->parse_cb,
                                               "s#O",
                                               buffer, received_size,
                                               req->acc);
        if(parse_response == NULL) {
            goto error;
        }

        if(!PyArg_ParseTuple(parse_response, "OO", &done_bool, &newacc)) {
            goto error;
        }

        // we own these now
        Py_INCREF(done_bool);
        Py_INCREF(newacc);

        if(PyObject_IsTrue(done_bool)) {
            // we're done!
            done_none_result = PyObject_CallFunctionObjArgs(req->done_cb, newacc, NULL);

            if(done_none_result == NULL) {
                goto error;
            }

            // all done!
            goto bailout;
        }

        // otherwise done_bool is Falsy so we need to replace acc with newacc
        // and call him again with more data
        Py_DECREF(req->acc);
        req->acc = newacc;
        // now there are two references here: the struct's, and this function's.
        // this function will release his on cleanup
        Py_INCREF(req->acc);

        // we're still listening for reads, so we'll just get called again when
        // there's more data available
    }

    goto cleanup;

error:
    // there's an exception on the stack that occurred that we can report back
    // up through the cb
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);
    // since we're actually "handling" it, we can normalise it
    PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);

    error_none_result = PyObject_CallFunction(req->done_cb, "((sO))", "error", pvalue);

    Py_XDECREF(error_none_result);
    Py_XDECREF(ptype);
    Py_XDECREF(pvalue);
    Py_XDECREF(ptraceback);

bailout:
    // we're totally done so free everything up now
    Py_DECREF(req->connection);
    Py_DECREF(req->body);
    Py_DECREF(req->parse_cb);
    Py_DECREF(req->acc);
    Py_DECREF(req->done_cb);

    free(req);

    ev_io_stop(loop, watcher);
    free(watcher);

cleanup:
    Py_XDECREF(newacc);
    Py_XDECREF(done_bool);
    Py_XDECREF(parse_response);
    Py_XDECREF(done_none_result);

    if(PyErr_Occurred()) {
        // we've already cleaned up but if there's still an exception there's no
        // way to bubble this back up, so the best we can do is print and clear
        // it. TODO we should invalidate this connection so a new one can be
        // established
        PyErr_Print();
    }

    PyGILState_Release(gstate);
}

static PyObject* _MemcevClient__getset_request(_MemcevClient *self, PyObject *args) {
    // memcached requests are always request->response, so this abstracts that
    // pattern while allowing the parsing to be done in Python where it's easier

    PyObject* connection_obj = NULL;
    PyObject* body = NULL;
    PyObject* parse_cb = NULL;
    PyObject* done_cb = NULL;
    PyObject* acc = NULL;

    ev_io* watcher = NULL;
    getset_request* req = NULL;

    if(!PyArg_ParseTuple(args, "OSOOO",
                         &connection_obj, &body, &parse_cb, &acc, &done_cb)) {
        return NULL;
    }

    // we're going to hold onto these in C so we'd better have a reference
    Py_INCREF(connection_obj);
    Py_INCREF(body);
    Py_INCREF(parse_cb);
    Py_INCREF(done_cb);
    Py_INCREF(acc);

    ev_connection* connection = PyCapsule_GetPointer(connection_obj, "connection");
    if(connection == NULL) {
        goto error;
    }

    if((watcher = malloc(sizeof(ev_async))) == NULL) {
        PyErr_NoMemory();
        goto error;
    }

    ev_io_init(watcher, getset_request_cb, connection->fd, EV_WRITE);

    if((req = malloc(sizeof(getset_request))) == NULL) {
        PyErr_NoMemory();
        goto error;
    }

    req->connection = connection_obj;
    req->body = body;
    req->parse_cb = parse_cb;
    req->acc = acc;
    req->done_cb = done_cb;
    req->state = getset_not_started;

    watcher->data = req;

    ev_io_start(self->loop, watcher);

    Py_RETURN_NONE;

error:
    Py_DECREF(connection_obj);
    Py_DECREF(body);
    Py_DECREF(parse_cb);
    Py_DECREF(done_cb);
    Py_DECREF(acc);

    if(watcher != NULL) {
        free(watcher);
    }
    if(req != NULL) {
        free(req);
    }

    // there's an exception already on the stack if we're down here
    return NULL;
}

static void free_connection_capsule(PyObject *capsule) {
    ev_connection* connection = PyCapsule_GetPointer(capsule, "connection");
    if(connection == NULL) {
        return;
    }

    // if the capsule has lost all references, there can be no more watchers or
    // anything so this should be safe

    // this may block because it may have to flush anything in the socket. in
    // real life it shouldn't though
    Py_BEGIN_ALLOW_THREADS;

    close(connection->fd);

    Py_END_ALLOW_THREADS;

    free(connection);
}

static void connect_cb(struct ev_loop* loop, ev_io *watcher, int revents) {
    if(!(EV_WRITE & revents)) {
        return;
    }

    // clean all of this up first and extract just the connection and callback
    ev_io_stop(loop, watcher);
    connect_request* request = watcher->data;
    ev_connection* connection = request->connection;
    PyObject* callback = request->callback;
    free(request);
    free(watcher);

    int so_error;
    socklen_t len = sizeof(so_error);
    int sockopt_result = getsockopt(connection->fd, SOL_SOCKET,
                                    SO_ERROR, &so_error, &len);

    if(sockopt_result != -1 && so_error == 0) {
        // success!
        connection->state = connection_connected;
    } else {
        connection->state = connection_error;
        if(sockopt_result == -1) {
            connection->error = strerror(errno);
        } else {
            connection->error = strerror(so_error);
        }
        close(connection->fd);
    }

    // now grab the GIL so we can tell the client about it
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    // call the callback with either an error tuple or a success tuple
    // containing PyCapsule containing a connection*

    PyObject* none_result = NULL;
    PyObject* capsule = NULL;

    if(connection->state == connection_error) {
        none_result = PyObject_CallFunction(callback, "((ss))", "error", connection->error);
        if(none_result == NULL) {
            goto sock_cleanup;
        }
    } else {
        capsule = PyCapsule_New(connection, "connection", free_connection_capsule);
        if(capsule == NULL) {
            goto sock_cleanup;
        }

        none_result = PyObject_CallFunction(callback, "((sO))", "connected", capsule);

        if(none_result == NULL) {
            // if he didn't exit successfully, we don't really know if he did
            // his job to add the capsule to the connections queue, so we don't
            // know whether we should close the socket. but since we're this
            // far, we know that the capsule has been created successfully so
            // can rely on refcounting to close it from now on
            goto sock_cleanup;
        }
    }

    goto cleanup;

sock_cleanup:
    // an error occurred that means we need to clean up the socket first
    close(connection->fd);
    free(connection);

cleanup:
    Py_DECREF(callback);
    Py_XDECREF(capsule);
    Py_XDECREF(none_result);

    if(PyErr_Occurred()) {
        // not much else I can do here, we're on the event loop's thread
        PyErr_Print();
    }

    PyGILState_Release(gstate);
}

static PyObject* _MemcevClient__connect(_MemcevClient *self, PyObject *args) {
    PyObject* ret = NULL;
    PyObject* cb = NULL;
    char* hostname = NULL;
    int port = 0;

    if (!PyArg_ParseTuple(args, "siO",
                          &hostname, &port, &cb)) {
        return NULL;
    }

    // cb will escape this function, even though hostname and port will not
    Py_INCREF(cb);

    ev_connection* connection = NULL;
    ev_io* connect_watcher = NULL;
    connect_request* request = NULL;

    connect_watcher = malloc(sizeof(ev_io));
    if(connect_watcher == NULL) {
        ret = PyErr_NoMemory();
        goto error;
    }

    request = malloc(sizeof(connect_request));
    if(request == NULL) {
        ret = PyErr_NoMemory();
        goto error;
    }

    // he's going to do a blocking network call to resolve DNS
    Py_BEGIN_ALLOW_THREADS;

    connection = make_connection(hostname, port);

    Py_END_ALLOW_THREADS;

    if(connection == NULL) {
        ret = PyErr_NoMemory();
        goto error;
    }

    if(connection->state == connection_error) {
        // since we're connecting in a non-blocking way, these errors can only
        // be errors in DNS resolution or allocation failures. we don't find out
        // about any others until later
        PyErr_SetString(PyExc_RuntimeError, connection->error);
        goto error;
    }

    connect_watcher->data = request;
    request->connection = connection;
    request->callback = cb;

    ev_io_init(connect_watcher, connect_cb, connection->fd, EV_WRITE);
    ev_io_start(self->loop, connect_watcher);

    Py_RETURN_NONE;

error:
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
        ret->state = connection_error;
        ret->error = strerror(errno);

        goto cleanup;
    }

    ret->fd = sock;
    ret->state = connection_connecting;

    /* set it non-blocking */
    if(-1 == fcntl(sock, F_SETFL, O_NONBLOCK | fcntl(sock, F_GETFL))) {
        ret->state = connection_error;
        ret->error = strerror(errno);

        goto cleanup;
    }

    // libev doesn't have an async DNS implementation like libevent, and I don't
    // really want to pull in libadns as a dependency. but we won't be doing
    // this very often anyway and connections are already blocking from the
    // caller's perspective
    struct hostent* he = gethostbyname(host);
    if(he == NULL) {
        ret->state = connection_error;
        ret->error = (char*)hstrerror(h_errno);
        goto cleanup;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);

    memcpy(&server.sin_addr, he->h_addr_list[0], he->h_length);

    int connect_status = connect(sock, (struct sockaddr *)&server, sizeof(server));

    if(connect_status == 0) {
        ret->state = connection_error;
        ret->error = "unexpected success";
    } else if(errno != EINPROGRESS) {
        // anything else is wrong
        ret->state = connection_error;
        ret->error = strerror(errno);
    }

cleanup:
    if(ret->state == connection_error && sock != -1) {
        close(sock);
    }

    // hostents are cleaned up/reused by the system

    return ret;
}

static int _MemcevClient_init(_MemcevClient *self, PyObject *args, PyObject *kwargs) {
    int ret = 0;

    static char *kwdlist[] = {NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "",
                                     kwdlist)) {
        // we take no arguments because our superclass is expected to handle
        // them
        return -1;
    }

    // we have to initialise this here instead of letting the eventloop thread
    // do it, because we need a handle to it and to be able to promise that it
    // can be called before we can promise that the event loop has initialised
    // it
    self->loop = ev_loop_new(EVFLAG_AUTO);
    if(self->loop == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Couldn't power up ev_loop_new");
        ret = -1;
        goto cleanup;
    }

    ev_async_init(&self->async_watcher, notify_event_loop);
    ev_async_start(self->loop, &self->async_watcher);

    /* give that watcher access to our struct */
    ev_set_userdata(self->loop, self);

cleanup:

    return ret;
}

static void _MemcevClient_dealloc(_MemcevClient* self) {
    // this isn't called until the event loop finishes running, so it should be
    // safe to clean up everything including the libev objects
    if(self->loop != NULL) {
        ev_loop_destroy(self->loop);
        self->loop = NULL;
    }

    // the async_watcher has no cleanup method, so I think it's safe to assume
    // that it has no state after it's not used?

    self->ob_type->tp_free((PyObject*)self);
}


PyMODINIT_FUNC init_memcev(void) {
    // initialise the module

    PyObject* module;

    // have to do this here because some C compilers have issues with static
    // references between modules. we can take this out when we make our own
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
