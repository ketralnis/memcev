#ifndef __MEMCEV_H__
#define __MEMCEV_H__

#include <Python.h>
#include <structmember.h>

#ifndef EV_MULTIPLICITY
#error "We require EV_MULTIPLICITY to run"
#endif

/* prototypes */

typedef enum {
    connecting
} ev_state;

typedef struct {
    int fd;
    ev_state state;
} ev_connection;

typedef struct {
    PyObject_HEAD
    /* our own C-visible fields go here. */

    PyStringObject* host;

    int port;
    int size;
    PyObject* connections; // a Queue of PyCapsule of ev_connection
    PyObject* requests; // a Queue of work tuples
    PyObject* thread;

    ev_async async_watcher;
    struct ev_loop *loop;
} _MemcevClient;

PyMODINIT_FUNC init_memcev(void);
static PyObject * _MemcevClient_notify(_MemcevClient *self, PyObject *unused);
static PyObject * _MemcevClient_start(_MemcevClient *self, PyObject *unused);
static PyObject * _MemcevClient_stop(_MemcevClient *self, PyObject *unused);
static int _MemcevClient_init(_MemcevClient *self, PyObject *args, PyObject *kwds);
static void _MemcevClient_dealloc(_MemcevClient* self);

static void notify_event_loop(struct ev_loop *loop, ev_async *watcher, int revents);
// static ev_connection make_connection(char* host, int port);

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
    {NULL, NULL, 0, NULL}
};

static PyMemberDef _MemcevClientType_members[] = {
    /* we handle them all internally for now */
    {"host",        T_OBJECT, offsetof(_MemcevClient, host),        0, "the host to connect to"},
    {"port",        T_INT,    offsetof(_MemcevClient, port),        0, "the port to connect to"},
    {"size",        T_INT,    offsetof(_MemcevClient, size),        0, "how many connections to maintain"},
    {"connections", T_OBJECT, offsetof(_MemcevClient, connections), 0, "internal connection pool"},
    {"requests",    T_OBJECT, offsetof(_MemcevClient, requests),    0, "internal requests queue"},
    {"thread",      T_OBJECT, offsetof(_MemcevClient, thread),      0, "internal pointer to thread object"},
    {NULL}
};

/* the actual class */
static PyTypeObject _MemcevClientType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "_memcev._MemcevClient",       /* tp_name */
    sizeof(_MemcevClient),  /* tp_basicsize */
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
    _MemcevClientType_methods,  /* tp_methods */
    _MemcevClientType_members,  /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)_MemcevClient_init, /* tp_init */
};

#endif /* __MEMCEV_H__ */