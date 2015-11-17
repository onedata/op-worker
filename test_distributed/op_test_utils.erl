%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module contains utility function used in ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(op_test_utils).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([session_setup/5, session_teardown/2]).
-export([remove_pending_messages/0, remove_pending_messages/1, clear_models/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new test session.
%% @end
%%--------------------------------------------------------------------
-spec session_setup(Worker :: node(), SessId :: session:id(),
    Iden :: session:identity(), Con :: pid(), Config :: term()) -> NewConfig :: term().
session_setup(Worker, SessId, Iden, Con, Config) ->
    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Con])),
    [{session_id, SessId}, {identity, Iden} | Config].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes existing test session.
%% @end
%%--------------------------------------------------------------------
-spec session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
session_teardown(Worker, Config) ->
    SessId = proplists:get_value(session_id, Config),
    ?assertEqual(ok, rpc:call(Worker, session_manager, remove_session, [SessId])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv remove_pending_messages(0)
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages() -> ok.
remove_pending_messages() ->
    remove_pending_messages(0).

%%--------------------------------------------------------------------
%% @doc
%% Removes messages for process queue. Waits 'Timeout' milliseconds for the
%% next message.
%% @end
%%--------------------------------------------------------------------
-spec remove_pending_messages(Timeout :: timeout()) -> ok.
remove_pending_messages(Timeout) ->
    receive
        _ -> remove_pending_messages(Timeout)
    after
        Timeout -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes all records from models.
%% @end
%%--------------------------------------------------------------------
-spec clear_models(Worker :: node(), Names :: [atom()]) -> ok.
clear_models(Worker, Names) ->
    lists:foreach(fun(Name) ->
        {ok, Docs} = ?assertMatch({ok, _}, rpc:call(Worker, Name, list, [])),
        lists:foreach(fun(#document{key = Key}) ->
            ?assertEqual(ok, rpc:call(Worker, Name, delete, [Key]))
        end, Docs)
    end, Names).

%%%===================================================================
%%% Internal functions
%%%===================================================================