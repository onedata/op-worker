%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating archives in CT tests.
%%% TODO - VFS-8382 investigate low performance of archives functions
%%% @end
%%%-------------------------------------------------------------------
-module(opt_archives).
-author("Bartosz Walkowicz").

-export([
    list/4, list/5,
    archive_dataset/5, archive_dataset/7,
    get_info/3,
    update/4,
    init_purge/3, init_purge/4
]).

-define(CALL(NodeSelector, Args), test_rpc:call(
    op_worker, NodeSelector, opl_archives, ?FUNCTION_NAME, Args  %% TODO add timeout?
)).


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
