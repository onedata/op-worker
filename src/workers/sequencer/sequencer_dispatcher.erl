
%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_dispatcher).
-author("Krzysztof Trzepla").

-include("workers/datastore/datastore_models.hrl").

%% API
-export([create_or_get_sequencer_manager/2, remove_sequencer_manager/1]).

%%%===================================================================
%%% API
%%%===================================================================

create_or_get_sequencer_manager(FuseId, Connection) ->
    case get_sequencer_manager(FuseId) of
        {ok, #sequencer_manager_model{pid = SeqMan}} ->
            ok = gen_server:call(SeqMan, {add_connection, Connection}),
            {ok, SeqMan};
        {error, {not_found, _}} ->
            create_sequencer_manager(FuseId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.

remove_sequencer_manager(FuseId) ->
    case get_sequencer_manager(FuseId) of
        {ok, #sequencer_manager_model{node = Node, sup = SeqManSup}} ->
            ok = sequencer_dispatcher_sup:stop_sequencer_manager_sup(Node, SeqManSup),
            sequencer_manager_model:delete(FuseId);
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_sequencer_manager(FuseId) ->
    case sequencer_manager_model:get(FuseId) of
        {ok, #document{value = SeqModel}} ->
            {ok, SeqModel};
        {error, Reason} ->
            {error, Reason}
    end.

create_sequencer_manager(FuseId, Connection) ->
    Node = node(),
    {ok, SeqManSup} = sequencer_dispatcher_sup:start_sequencer_manager_sup(),
    {ok, SeqSup} = sequencer_manager_sup:start_sequencer_sup(SeqManSup),
    {ok, SeqMan} = sequencer_manager_sup:start_sequencer_manager(SeqManSup, SeqSup, Connection),
    case sequencer_manager_model:create(#document{key = FuseId, value = #sequencer_manager_model{
        node = Node, pid = SeqMan, sup = SeqManSup
    }}) of
        {ok, FuseId} ->
            {ok, SeqMan};
        {error, already_exists} ->
            ok = sequencer_dispatcher_sup:stop_sequencer_manager_sup(Node, SeqManSup),
            create_or_get_sequencer_manager(FuseId, Connection);
        {error, Reason} ->
            {error, Reason}
    end.