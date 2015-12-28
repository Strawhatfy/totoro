Totoro
========

Celery integration with Tornado.

Installation
------------

You can install Totoro either via the Python Package Index (PyPI) or from source.

To install using pip, simply:

.. code-block:: bash

    $ sudo pip install totoro

or alternatively (you really should be using pip though):

.. code-block:: bash

    $ sudo easy_install totoro

or from source:

.. code-block:: bash

    $ sudo python setup.py install

Totoro can only support AMQP by default. To use the redis, you can specify the requirements on the pip comand-line by using brackets.

.. code-block:: bash

    $ sudo pip install totoro[redis]

Getting Started
------------

.. code-block:: python
