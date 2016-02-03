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
-define(run(B), datastore_runner:run_and_normalize_error(fun() -> B end, ?MODULE)).

-endif.