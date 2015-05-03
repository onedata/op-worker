%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file defines macros used during basic datastore tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_BASIC_OPS_UTILS_HRL).
-define(DATASTORE_BASIC_OPS_UTILS_HRL, 1).

-define(repeats, 1).

-define(basic_test_def(Desc),
    -performance([
        {repeats, ?repeats},
        {parameters, [
            [{name, threads_num}, {value, 20}, {description, "Number of threads that operates at single node."}],
            [{name, docs_per_thead}, {value, 5}, {description, "Number of documents used by single threads."}],
            [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
            [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
        ]},
        {description, Desc},
        {config, [{name, single_short_thread},
            {parameters, [
                [{name, threads_num}, {value, 1}],
                [{name, docs_per_thead}, {value, 10}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, single_long_thread},
                    {parameters, [
                        [{name, threads_num}, {value, 1}],
                        [{name, docs_per_thead}, {value, 1000}],
                        [{name, conflicted_threads}, {value, 1}]
                    ]}
        ]},
        {config, [{name, single_long_thread_one_op_per_doc},
            {parameters, [
                [{name, threads_num}, {value, 1}],
                [{name, docs_per_thead}, {value, 5000}],
                [{name, ops_per_doc}, {value, 1}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, multiple_threads_no_conflicts},
                    {parameters, [
                        [{name, threads_num}, {value, 500}],
                        [{name, docs_per_thead}, {value, 500}],
                        [{name, ops_per_doc}, {value, 1}],
                        [{name, conflicted_threads}, {value, 1}]
                    ]}
        ]},
        {config, [{name, multiple_threads_with_repeats},
                    {parameters, [
                        [{name, threads_num}, {value, 500}],
                        [{name, docs_per_thead}, {value, 500}],
                        [{name, conflicted_threads}, {value, 1}]
                    ]}
        ]},
        {config, [{name, multiple_threads_with_conflits},
                    {parameters, [
                        [{name, threads_num}, {value, 500}],
                        [{name, docs_per_thead}, {value, 500}],
                        [{name, conflicted_threads}, {value, 25}]
                    ]}
        ]}
    ])
).

-define(create_delete_test_def,
    ?basic_test_def("Performs multipe create/delete operations using many threads.")
).

-define(save_test_def,
    ?basic_test_def("Performs multipe saves operations using many threads. "
        "Document may be saved many times.")
).

-define(update_test_def,
    ?basic_test_def("Performs multipe update operations using many threads. "
        "Document may be updated many times.")
).

-define(get_test,
    ?basic_test_def("Performs multipe get operations using many threads.")
).

-define(exists_test,
    ?basic_test_def("Performs multipe exists operations using many threads.")
).

-endif.