#include <Python.h>
#include <ev.h>

#include "_memcevmodule.h"

static PyObject* _MemcevEventLoop_start(_MemcevEventLoop *self, PyObject *unused) {
    /* TODO this is the function called in its own Thread */
    Py_RETURN_NONE;
}


static PyObject* _MemcevEventLoop_notify(_MemcevEventLoop *self, PyObject *unused) {
    /* TODO */
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

static int _MemcevEventLoop_init(_MemcevEventLoop *self, PyObject *args, PyObject *kwargs) {
    unsigned int size;
    char* host;
    int port;

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

    for(int x=0; x<size; x++) {
        /* these will be built and connected by the eventloop thread, so make
           sure that the first thing that he does when he comes up is connect to
           them */

        PyObject* none_result = PyObject_CallMethod(self->requests, "put",
                                                    "(ssi)", "connect", host, port);
        Py_XDECREF(none_result);

        if(none_result == NULL) {
            ret = -1;
            goto cleanup;
        }
    }

cleanup:

    return ret;
}

static void _MemcevEventLoop_dealloc(_MemcevEventLoop* self) {
    Py_XDECREF(self->requests);
    self->ob_type->tp_free((PyObject*)self);
}


PyMODINIT_FUNC init_memcev(void) {
    /* initialise the module */

    PyObject* module;

    /* initalise the EventLoop type */

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
