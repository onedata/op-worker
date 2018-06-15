%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is used for communication between providers that
%%% take part in file_replica invalidation.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_communicator).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

%% API
-export([
    request_eviction_support/7,
    confirm_invalidation_support/2,
    refuse_invalidation_support/1,
    release_invalidation_lock/1,
    notify_successful_invalidation/3,
    notify_failed_invalidation/3
]).

-define(INVALIDATION_TIMEOUT, application:get_env(?APP_NAME,
    invalidation_timeout, timer:minutes(1))).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Requests invalidation support from given provider.
%% Request is synchronous, process calling this function waits for
%% result for timeout ?INVALIDATION_TIMEOUT.
%% @end
%%-------------------------------------------------------------------
%%-spec request_eviction_support(file_meta:uuid(), fslogic_block:blocks(),
%%    version_vector:version_vector(), od_provider:id(), od_space:id()) ->
%%    {ok, non_neg_integer()} | {error, term()}.
request_eviction_support(FileUuid, FileBlocks, VV, ProviderId, SpaceId, Type, Id) ->
    replica_eviction:request(FileUuid, FileBlocks, VV, ProviderId, SpaceId, Type, Id).

confirm_invalidation_support(MsgId, Blocks) ->
    replica_eviction:confirm(MsgId, Blocks).

refuse_invalidation_support(MsgId) ->
    replica_eviction:refuse(MsgId).

release_invalidation_lock(MsgId) ->
    replica_eviction:release_lock(MsgId).

notify_successful_invalidation(Pid, MsgId, InvalidatedBytes) ->
    DecodedPid = transfer_utils:decode_pid(Pid),
    DecodedPid ! {invalidation_finished, MsgId, InvalidatedBytes}.

notify_failed_invalidation(Pid, MsgId, Error) ->
    DecodedPid = transfer_utils:decode_pid(Pid),
    DecodedPid ! {invalidation_failed, MsgId, Error}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

