%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs multipart upload operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_multipart_upload).

-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    create/3, abort/2, complete/2, list/4,
    upload_part/3, list_parts/4
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(session:id(), od_space:id(), file_meta:path()) -> 
    {ok, multipart_upload:id()} | {error, term()}.
create(SessId, SpaceId, Path) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #create_multipart_upload{
        space_id = SpaceId,
        path = Path
    },
        fun(#multipart_upload{multipart_upload_id = UploadId}) ->
            {ok, UploadId}
        end).


-spec abort(session:id(), multipart_upload:id()) -> ok | {error, term()}.
abort(SessId, UploadId) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #abort_multipart_upload{
        multipart_upload_id = UploadId
    }, fun(_) -> ok end).


-spec complete(session:id(), multipart_upload:id()) -> ok | {error, term()}.
complete(SessId, UploadId) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #complete_multipart_upload{
        multipart_upload_id = UploadId
    }, fun(_) -> ok end).


-spec list(session:id(), od_space:id(), non_neg_integer(), multipart_upload:pagination_token() | undefined) ->
    {ok, [multipart_upload:record()], multipart_upload:pagination_token() | undefined, boolean()} | {error, term()}.
list(SessId, SpaceId, Limit, Token) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #list_multipart_uploads{
        space_id = SpaceId,
        limit = Limit,
        index_token = Token
    }, fun(#multipart_uploads{
        uploads = Uploads,
        next_page_token = NextPageToken,
        is_last = IsLast
    }) -> {ok, Uploads, NextPageToken, IsLast} end).


-spec upload_part(session:id(), multipart_upload:id(), multipart_upload_part:record()) ->
    ok | {error, term()}.
upload_part(SessId, UploadId, Part) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #upload_multipart_part{
        multipart_upload_id = UploadId,
        part = Part
    }, fun(_) -> ok end).


-spec list_parts(session:id(), multipart_upload:id(), non_neg_integer(), multipart_upload_part:part_number()) ->
    {ok, [multipart_upload_part:record()], boolean()} | {error, term()}.
list_parts(SessId, UploadId, Limit, StartAfter) ->
    remote_utils:call_fslogic(SessId, multipart_upload_request, #list_multipart_parts{
        multipart_upload_id = UploadId,
        limit = Limit,
        part_marker = StartAfter
    }, fun(#multipart_parts{
        parts = Parts,
        is_last = IsLast
    }) -> {ok, Parts, IsLast} end).
