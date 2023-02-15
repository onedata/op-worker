%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Macros for running datastore models functions
%%% @end
%%%--------------------------------------------------------------------
-ifndef(DATASTORE_RUNNER_HRL).
-define(DATASTORE_RUNNER_HRL, 1).

%% Runs given codeblock and converts any badmatch/case_clause to {error, Reason :: term()}
-define(run(Fun), datastore_runner:run_and_normalize_error(
    fun() -> Fun end
)).

-define(extract_ok(Result), datastore_runner:extract_ok(Result)).
-define(extract_key(Result), datastore_runner:extract_key(Result)).
-define(ok_if_not_found(Result), datastore_runner:ok_if_not_found(Result)).
-define(ok_if_exists(Result), datastore_runner:ok_if_exists(Result)).
-define(ok_if_no_change(Result), datastore_runner:ok_if_no_change(Result)).

-define(get_field(Key, GetterFun),
    datastore_runner:get_field(Key, ?MODULE, GetterFun)).

-endif.