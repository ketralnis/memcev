#ifndef __MEMCEV_H__
#define __MEMCEV_H__

#include <Python.h>
#include <structmember.h>

#ifndef EV_MULTIPLICITY
#error "We require EV_MULTIPLICITY to run"
#endif

#if PY_VERSION_HEX < 0x02070000
#error "We require Python 2.7 to run"
#endif

/* prototypes */

typedef struct {
    PyObject_HEAD
    /* our own C-visible fields go here. */

    ev_async async_watcher;
    struct ev_loop *loop;
} _MemcevClient;

typedef enum {
    connection_not_started,
    connection_connecting,
    connection_error,
    connection_connected,
} ev_connection_state;

typedef struct {
    int fd;
    ev_connection_state state;
    char* error;
} ev_connection;

typedef struct {
    ev_connection* connection;
    PyObject* callback;
} connect_request;

typedef enum {
    get_not_started, // we're waiting for the connection to become writeable
    get_awaiting_response, // we sent the request and are waiting for the response
} get_request_state;

typedef struct {
    PyObject* cb; // who to call with the result
    PyObject* key;
    PyObject* connection; // capsule containing the ev_connection
    PyObject* acc; // somewhere for _parse_get_response to store its intermediate states
    get_request_state state;
} get_request;

typedef struct {
    ev_connection* connection;
    PyObject* callback;
} set_request;

PyMODINIT_FUNC init_memcev(void);
static PyObject* _MemcevClient_notify(_MemcevClient *self, PyObject *unused);
static PyObject* _MemcevClient_start(_MemcevClient *self, PyObject *unused);
static PyObject* _MemcevClient_stop(_MemcevClient *self, PyObject *unused);
static PyObject* _MemcevClient__connect(_MemcevClient *self, PyObject *args);
static PyObject* _MemcevClient__get(_MemcevClient *self, PyObject *args);
static PyObject* _MemcevClient__set(_MemcevClient *self, PyObject *args);

static int _MemcevClient_init(_MemcevClient *self, PyObject *args, PyObject *kwds);
static void _MemcevClient_dealloc(_MemcevClient* self);

static void notify_event_loop(struct ev_loop *loop, ev_async *watcher, int revents);
static ev_connection* make_connection(char* host, int port);

/* the method table */
static PyMethodDef _MemcevClientType_methods[] = {
    /* Python-visible methods go here */
    {
        "notify",
        (PyCFunction)_MemcevClient_notify, METH_NOARGS,
        "we've added something to the requests queue, notify the eventloop"
    },
    {
        "start",
        (PyCFunction)_MemcevClient_start, METH_NOARGS,
        "start the eventloop (probably in its own Thread)"
    },
    {
        "stop",
        (PyCFunction)_MemcevClient_stop, METH_NOARGS,
        "stop the eventloop (forcefully)"
    },

    {
        "_connect",
        (PyCFunction)_MemcevClient__connect, METH_VARARGS,
        "make an new connection (internal C implementation)"
    },

    {
        "_get",
        (PyCFunction)_MemcevClient__get, METH_VARARGS,
        "set a key to a value (internal C implementation)"
    },

    {
        "_set",
        (PyCFunction)_MemcevClient__set, METH_VARARGS,
        "get a key's value (internal C implementation)"
    },

    {NULL, NULL, 0, NULL}
};

static PyMemberDef _MemcevClientType_members[] = {
    /* we handle them all internally for now */
    {NULL}
};

/* the actual class */
static PyTypeObject _MemcevClientType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "_memcev._MemcevClient",   /* tp_name */
    sizeof(_MemcevClient),     /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)_MemcevClient_dealloc,  /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /* tp_flags */
    "A memcev client. Don't use me directly, use memcev.Client.", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _MemcevClientType_methods, /* tp_methods */
    _MemcevClientType_members, /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)_MemcevClient_init, /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new, will set in init_memcev */
};

#endif /* __MEMCEV_H__ */