%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definions and configurations for datastore.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_HRL).
-define(DATASTORE_HRL, 1).

-include("modules/datastore/datastore_models.hrl").

%% Common predicates
-define(PRED_ALWAYS, fun() -> true end).


%% Utils
-define(RESPONSE(R), begin
                         {ok, Response} = R,
                         Response
                     end
).

%% Common funs
-define(GET_ALL,
    fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (Obj, Acc) ->
            {next, [Obj | Acc]}
    end).

-endif.
