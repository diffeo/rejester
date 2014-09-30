'''Redis-based distributed work manager.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Rejester is a distributed work manager, using the `Redis`_ in-memory
database as shared state.  In typical use, some number of systems run
rejester *worker* processes, which query the central database to find
new work; anything that can reach the database can submit jobs.  The
standard rejester work system can also directly deliver work units to
programs without using the dedicated worker process.  For specialized
applications there is also a globally atomic string-based priority
queue.

To use a rejester-based application:

1. Create a YAML configuration file as described below.

2. Run ``rejester mode run`` once on any system to enable rejester.

3. Run :command:`rejester_worker` on some number of systems with
   identical copies of the configuration file.  These may be anywhere,
   but must be able to reach the `Redis`_ server.  Each will start as
   many workers as the system has CPU cores.

4. Run some tool that generates rejester work units.  For instance,
   the :command:`streamcorpus_directory` tool included in
   `streamcorpus-pipeline`_ generates work units to process text
   files.

5. Run ``rejester summary`` and other sub-commands of the
   :command:`rejester` tool to get information about the job's progress.

6. Run ``rejester mode terminate`` to instruct all of the workers to
   shut down.

.. _Redis: http://redis.io/
.. _streamcorpus-pipeline: https://github.com/trec-kba/streamcorpus-pipeline

:command:`rejester` tool
========================

.. automodule:: rejester.run

:command:`rejester_worker` tool
===============================

.. automodule:: rejester.run_multi_worker

.. currentmodule:: rejester

Configuration
=============

Rejester uses :mod:`yakonfig` for its configuration.  The relevant
section of the configuration file looks like:

.. code-block:: yaml

    rejester:
      # These two options are required
      registry_addresses: [ "redis.example.com:6379" ]
      namespace: rejester

      # The following are optional, defaults shown
      app_name: rejester
      default_lifetime: 900
      enough_memory: false
      worker: fork_worker

`registry_addresses` indicates the location of the `Redis`_ server.
While this is a list, only the first value is used.  Also note that
YAML syntax requires quoting the ``host:port`` string, lest it be
interpreted as a dictionary.

`app_name` and `namespace` identify the specific application in use.
Multiple applications can share the same `Redis`_ server so long as
they have distinct namespace strings.  `app_name` is currently fixed
at ``rejester`` and setting a different value has no effect.

`default_lifetime` indicates how long a job is allowed to run before
it must call :meth:`rejester._task_master.WorkUnit.update`.  If a
job runs beyond this timeout it will return from the "pending" list to
the "available" list, and another worker may start working on it.

When a worker requests a job, if the system does not have enough memory
to satisfy a given work spec's ``min_gb`` request, that work spec is
skipped.  Setting `enough_memory` to true overrides this check.

`worker` specifies the worker implementation to use in
:command:`rejester_worker`.  Valid options are ``fork_worker`` or
``multi_worker``.  ``fork_worker`` has additional configuration
options; see :class:`~rejester.workers.ForkWorker` for details.
``multi_worker`` is less stable and never allows jobs to time out, but
will start a set of jobs more quickly.

Task system
===========

Core API
--------

.. autoclass:: TaskMaster
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: WorkUnit
   :members:
   :undoc-members:
   :show-inheritance:

Workers
-------

.. autoclass:: Worker
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: rejester.workers
.. currentmodule:: rejester

Implementation Details
----------------------

.. autoclass:: Registry
   :members:
   :undoc-members:
   :show-inheritance:

Priority queue system
=====================

.. autoclass:: RejesterQueue
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions
==========

.. automodule:: rejester.exceptions
   :members:
   :undoc-members:
   :show-inheritance:



Example: StreamCorpus Simple Filter Stage
=========================================

Rejester makes it easy to run large batches of small jobs, such as a
couple million runs of tagging batches of 500 documents with an NER
tagger or filtering to smaller sets of documents.

As a simple example, we illustrate how to write a filter function as
an external stage and run it in AWS EC2.

:mod:`streamcorpus_pipeline` has several built-in filters `source in github
<https://github.com/trec-kba/streamcorpus-pipeline/blob/master/streamcorpus_pipeline/_filters.py>`_.
You can create your own as external stages.  For example, see this `exact name match filter
<https://github.com/trec-kba/streamcorpus-pipeline/blob/master/examples/filters_exact_match.py>`_.

.. code-block:: py

    ## use the newer regex engine
    import regex as re
    import string

    ## make a unicode translation table to converts all punctuation to white space
    strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

    white_space_re = re.compile("\s+")

    def strip_string(s):
        'strips punctuation and repeated whitespace from unicode strings'
        return white_space_re.sub(" ", s.translate(strip_punctuation).lower())


    class filter_exact_match(object):
        'trivial string matcher using simple normalization and regex'

        config_name = 'filter_exact_match'

        def __init__(self, config):
            path = config.get('match_strings_path')
            match_strings = open(path).read().splitlines()
            match_strings = map(strip_string, map(unicode, match_strings))
            self.matcher = re.compile("(.|\\n)*?(%s)" % '|'.join(match_strings), re.I)

        def __call__(self, si, context):
            'only pass StreamItems that match'
            if self.matcher.match(strip_string(si.body.clean_visible.decode('utf-8'))):
                return si


    ## this is how streamcorpus_pipeline finds the stage
    Stages = {'filter_exact_match': filter_exact_match}


To run this in rejester, you need to setup a `redis server
<http://redis.io/>`_ (use version 2.8 or newer), and put the hostname
in your yaml configuration file:

.. code-block:: yaml

    logging:
      root:
        level: INFO

    rejester:
      namespace: my_kba_filtering
      app_name: rejester
      registry_addresses: ["redis.example.com:6379"]

    streamcorpus_pipeline:
      ## "." means current working directory
      root_path: .

      tmp_dir_path: tmp
      cleanup_tmp_files: true

      external_stages_path: examples/filter_exact_match.py

      reader: from_s3_chunks

      incremental_transforms:
        ## remove all StreamItems that do not exactly match
        - filter_exact_match

      batch_transforms: []

      filter_exact_match:
        ## names ending in "_path" will be made absolute relative to the root_path
        match_strings_path: my_match_strings.txt

      writers: [to_s3_chunks]

      from_s3_chunks:
        ## put paths to your own keys here; these files must be just the
        ## access_key_id and secret_access_key strings without newlines
        aws_access_key_id_path:     /data/trec-kba/keys/trec-aws-s3.aws_access_key_id
        aws_secret_access_key_path: /data/trec-kba/keys/trec-aws-s3.aws_secret_access_key

        ## this is the location of the NIST's StreamCorpus
        bucket: aws-publicdatasets
        s3_path_prefix: trec/kba/kba-streamcorpus-2014-v0_3_0

        tries: 10
        input_format: streamitem
        streamcorpus_version: v0_3_0

        ## you need this key if you are processing NIST's StreamCorpus,
        ## which is encrypted
        gpg_decryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.private

      to_s3_chunks:

        ## put your own bucket and paths to your own keys here; these
        ## files must be just the access_key_id and secret_access_key
        ## strings without newlines
        aws_access_key_id_path:     /data/trec-kba/keys/trec-aws-s3.aws_access_key_id
        aws_secret_access_key_path: /data/trec-kba/keys/trec-aws-s3.aws_secret_access_key

        bucket: aws-publicdatasets
        s3_path_prefix: trec/kba/kba-streamcorpus-2014-v0_3_0-to-delete

        output_name: "%(date_hour)s/%(source)s-%(num)d-%(input_md5)s-%(md5)s"
        tries: 10
        cleanup_tmp_files: true

        ## you only need these keys if you are encrypting the data that
        ## you are putting into your bucket; you need both if you require
        ## verify_via_http, which fetches and decrypts to validate what
        ## was saved.
        gpg_decryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.private
        gpg_encryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.pub
        gpg_recipient: trec-kba

        verify_via_http: true


Then, you can run the following commands to put tasks into the rejester queue:

.. code-block:: bash

    ## populate the task queue with jobs
    streamcorpus_directory -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml --file-lists list_of_s3_paths.txt

    ## try running one, to make sure it works locally
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml run_one

    ## launch a MultiWorker to use all the CPUs on this machine:
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml run_worker

    ## check on the status of your jobs
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml summary


If you are interested in SaltStack states for spinning up machines to
do this, reach out on streamcorpus@googlegroups.com

'''
from __future__ import absolute_import
from rejester.config import config_name, default_config, add_arguments, \
    runtime_keys, discover_config, check_config
from rejester._task_master import TaskMaster, WorkUnit, Worker, build_task_master
from rejester._queue import RejesterQueue
from rejester._registry import Registry
