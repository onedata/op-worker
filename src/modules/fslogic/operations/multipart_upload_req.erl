%%%-------------------------------------------------------------------- 
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling multipart upload requests.
%%% @end
%%%--------------------------------------------------------------------
-module(multipart_upload_req).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([create/3, abort/2, complete/2, list/4]).
-export([upload_part/2, list_parts/3]).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(user_ctx:ctx(), od_space:id(), file_meta:path()) -> fslogic_worker:fuse_response().
create(UserCtx, SpaceId, Path) ->
    ?FUSE_OK_RESP(multipart_upload:create(SpaceId, user_ctx:get_user_id(UserCtx), Path)).


-spec abort(user_ctx:ctx(), multipart_upload:id()) -> fslogic_worker:fuse_response().
abort(UserCtx, UploadId) ->
    complete(UserCtx, UploadId).


-spec complete(user_ctx:ctx(), multipart_upload:id()) -> fslogic_worker:fuse_response().
complete(UserCtx, UploadId) ->
    case multipart_upload:finish(user_ctx:get_user_id(UserCtx), UploadId) of
        ok -> ?FUSE_OK_RESP;
        {error, not_found} -> #fuse_response{status = #status{code = ?EINVAL}}
    end.


-spec list(user_ctx:ctx(), od_space:id(), non_neg_integer(), multipart_upload:pagination_token() | undefined) ->
    fslogic_worker:fuse_response().
list(UserCtx, SpaceId, Limit, Token) ->
    {ok, Result, NextToken} = multipart_upload:list(SpaceId, user_ctx:get_user_id(UserCtx), Limit, Token),
    ?FUSE_OK_RESP(#multipart_uploads{
        uploads = Result,
        next_page_token = NextToken,
        is_last = NextToken == undefined
    }).


-spec upload_part(multipart_upload:id(), multipart_upload_part:part()) -> 
    fslogic_worker:fuse_response().
upload_part(UploadId, Part) ->
    case multipart_upload:get(UploadId) of
        {ok, _} -> 
            ok = multipart_upload_part:create(UploadId, Part),
            ?FUSE_OK_RESP;
        {error, not_found} ->
            #fuse_response{status = #status{code = ?EINVAL}}
    end.


-spec list_parts(multipart_upload:id(), non_neg_integer(), multipart_upload_part:part()) ->
    fslogic_worker:fuse_response().
list_parts(UploadId, Limit, StartAfter) ->
    case multipart_upload:get(UploadId) of
        {ok, _} ->
            {ok, Result, IsLast} = multipart_upload_part:list(UploadId, Limit, StartAfter),
            ?FUSE_OK_RESP(#multipart_parts{parts = Result, is_last = IsLast});
        {error, not_found} ->
            #fuse_response{status = #status{code = ?EINVAL}}
    end.
