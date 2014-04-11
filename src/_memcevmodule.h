#ifndef __MEMCEV_H__
#define __MEMCEV_H__

#include <Python.h>
#include <structmember.h>

/* prototypes */

typedef struct {
    PyObject_HEAD
    /* our own C-visible fields go here. */
    PyObject* requests; /* the queue of tuples of inbound requests */
} _MemcevEventLoop;

PyMODINIT_FUNC init_memcev(void);
static PyObject * _MemcevEventLoop_notify(_MemcevEventLoop *self, PyObject *unused);
static PyObject * _MemcevEventLoop_start(_MemcevEventLoop *self, PyObject *unused);
static int _MemcevEventLoop_init(_MemcevEventLoop *self, PyObject *args, PyObject *kwds);
static void _MemcevEventLoop_dealloc(_MemcevEventLoop* self);

/* the method table */
static PyMethodDef _MemcevEventLoopType_methods[] = {
    /* Python-visible methods go here */
    {
        "notify",
        (PyCFunction)_MemcevEventLoop_notify, METH_NOARGS,
        "we've added something to the requests queue, notify the eventloop"
    },
    {
        "start",
        (PyCFunction)_MemcevEventLoop_start, METH_NOARGS,
        "start the eventloop (probably in its own Thread)"
    },
    {NULL, NULL, 0, NULL}
};

static PyMemberDef _MemcevEventLoopType_members[] = {
    /* we handle them all internally for now */
    {NULL}
};

/* the actual class */
static PyTypeObject _MemcevEventLoopType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /* ob_size */
    "_memcev.EventLoop",       /* tp_name */
    sizeof(_MemcevEventLoop),  /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)_MemcevEventLoop_dealloc,  /* tp_dealloc */
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
    Py_TPFLAGS_DEFAULT,        /* tp_flags */
    "A memcev EventLoop. Don't use me directly, use memcev.Client.", /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _MemcevEventLoopType_methods,  /* tp_methods */
    _MemcevEventLoopType_members,  /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)_MemcevEventLoop_init, /* tp_init */
};

#endif /* __MEMCEV_H__ */