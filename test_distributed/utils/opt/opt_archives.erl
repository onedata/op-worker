%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating archives in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_archives).
-author("Bartosz Walkowicz").

-include("proto/oneprovider/provider_messages.hrl").

-export([
    list/4, list/5,
    archive_dataset/5, archive_dataset/7,
    cancel_archivisation/4,
    get_info/3,
    lookup_state/3,
    update/4,
    delete/3, delete/4,
    recall/5,
    cancel_recall/3,
    get_recall_details/3, get_recall_progress/3, browse_recall_log/4
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_archives, ?FUNCTION_NAME, Args, timer:minutes(3)) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    dataset_api:listing_opts()
) ->
    {ok, {archive_api:entries(), boolean()}} | errors:error().
list(NodeSelector, SessionId, DatasetId, Opts) ->
    list(NodeSelector, SessionId, DatasetId, Opts, undefined).


-spec list(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    archives_list:listing_opts(),
    undefined | archive_api:listing_mode()
) ->
    {ok, {archive_api:entries(), boolean()}} | errors:error().
list(NodeSelector, SessionId, DatasetId, Opts, ListingMode) ->
    ?CALL(NodeSelector, [SessionId, DatasetId, Opts, ListingMode]).


-spec archive_dataset(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    archive:config(),
    archive:description()
) ->
    {ok, archive:id()} | errors:error().
archive_dataset(NodeSelector, SessionId, DatasetId, Config, Description) ->
    archive_dataset(NodeSelector, SessionId, DatasetId, Config, undefined, undefined, Description).


-spec archive_dataset(
    oct_background:node_selector(),
    session:id(),
    dataset:id(),
    archive:config(),
    archive:callback(),
    archive:callback(),
    archive:description()
) ->
    {ok, archive:id()} | errors:error().
archive_dataset(
    NodeSelector, SessionId, DatasetId, Config, PreservedCallback, DeletedCallback, Description
) ->
    Result = ?CALL(NodeSelector, [
        SessionId, DatasetId, Config, PreservedCallback, DeletedCallback, Description
    ]),
    case Result of
        {ok, #archive_info{id = ArchiveId}} -> {ok, ArchiveId};
        {error, _} = Error -> Error
    end.


-spec cancel_archivisation(oct_background:node_selector(), session:id(), archive:id(), 
    archive:cancel_preservation_policy()) -> {ok, archive_api:info()} | errors:error().
cancel_archivisation(NodeSelector, SessionId, ArchiveId, PreservationPolicy) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, PreservationPolicy]).


-spec get_info(oct_background:node_selector(), session:id(), archive:id()) ->
    {ok, archive_api:info()} | errors:error().
get_info(NodeSelector, SessionId, ArchiveId) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId]).


-spec lookup_state(oct_background:node_selector(), session:id(), archive:id()) ->
    {ok, archive:state()} | errors:error().
lookup_state(NodeSelector, SessionId, ArchiveId) ->
    case get_info(NodeSelector, SessionId, ArchiveId) of
        {ok, #archive_info{state = State}} -> {ok, State};
        {error, _} = Error -> Error
    end.


-spec update(oct_background:node_selector(), session:id(), archive:id(), archive:diff()) ->
    ok | errors:error().
update(NodeSelector, SessionId, ArchiveId, Diff) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, Diff]).


-spec delete(oct_background:node_selector(), session:id(), archive:id()) ->
    ok | errors:error().
delete(NodeSelector, SessionId, ArchiveId) ->
    delete(NodeSelector, SessionId, ArchiveId, undefined).


-spec delete(oct_background:node_selector(), session:id(), archive:id(), archive:callback()) ->
    ok | errors:error().
delete(NodeSelector, SessionId, ArchiveId, CallbackUrl) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, CallbackUrl]).


-spec recall(oct_background:node_selector(), session:id(), archive:id(), file_id:file_guid(), 
    file_meta:name() | default) -> {ok, file_id:file_guid()} | errors:error().
recall(NodeSelector, SessionId, ArchiveId, TargetParentGuid, RootFileName) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, TargetParentGuid, RootFileName]).


-spec cancel_recall(oct_background:node_selector(), session:id(), file_id:file_guid()) -> 
    ok | errors:error().
cancel_recall(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).


-spec get_recall_details(oct_background:node_selector(), session:id(), file_id:file_guid()) ->
    {ok, archive_recall:record()} | {error, term()}.
get_recall_details(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).


-spec get_recall_progress(oct_background:node_selector(), session:id(), file_id:file_guid()) ->
    {archive_recall:recall_progress_map()} | {error, term()}.
get_recall_progress(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).


-spec browse_recall_log(oct_background:node_selector(), session:id(), file_id:file_guid(), 
    json_infinite_log_model:listing_opts()) -> 
    {json_infinite_log_model:browse_result()} | {error, term()}.
browse_recall_log(NodeSelector, SessionId, FileGuid, Options) ->
    ?CALL(NodeSelector, [SessionId, FileGuid, Options]).
