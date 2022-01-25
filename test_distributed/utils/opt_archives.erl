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

-export([
    list/4, list/5,
    archive_dataset/5, archive_dataset/7,
    get_info/3,
    update/4,
    init_purge/3, init_purge/4,
    init_recall/5, 
    get_recall_details/3, get_recall_progress/3
]).

-define(CALL(NodeSelector, Args),
    try test_rpc:call(op_worker, NodeSelector, mi_archives, ?FUNCTION_NAME, Args, timer:minutes(3)) of
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
    dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()
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
    NodeSelector, SessionId, DatasetId, Config, PreservedCallback, PurgedCallback, Description
) ->
    ?CALL(NodeSelector, [
        SessionId, DatasetId, Config, PreservedCallback, PurgedCallback, Description
    ]).


-spec get_info(oct_background:node_selector(), session:id(), archive:id()) ->
    {ok, archive_api:info()} | errors:error().
get_info(NodeSelector, SessionId, ArchiveId) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId]).


-spec update(oct_background:node_selector(), session:id(), archive:id(), archive:diff()) ->
    ok | errors:error().
update(NodeSelector, SessionId, ArchiveId, Diff) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, Diff]).


-spec init_purge(oct_background:node_selector(), session:id(), archive:id()) ->
    ok | errors:error().
init_purge(NodeSelector, SessionId, ArchiveId) ->
    init_purge(NodeSelector, SessionId, ArchiveId, undefined).


-spec init_purge(oct_background:node_selector(), session:id(), archive:id(), archive:callback()) ->
    ok | errors:error().
init_purge(NodeSelector, SessionId, ArchiveId, CallbackUrl) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, CallbackUrl]).


-spec init_recall(oct_background:node_selector(), session:id(), archive:id(), file_id:file_guid(), 
    file_meta:name() | default) -> {ok, file_id:file_guid()} | errors:error().
init_recall(NodeSelector, SessionId, ArchiveId, TargetParentGuid, RootFileName) ->
    ?CALL(NodeSelector, [SessionId, ArchiveId, TargetParentGuid, RootFileName]).


-spec get_recall_details(oct_background:node_selector(), session:id(), file_id:file_guid()) ->
    {ok, archive_recall_api:record()} | {error, term()}.
get_recall_details(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).


-spec get_recall_progress(oct_background:node_selector(), session:id(), file_id:file_guid()) ->
    {archive_recall_api:recall_progress_map()} | {error, term()}.
get_recall_progress(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).
