%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_multipart_upload_translator).
-author("Michal Stanisz").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'MultipartUploadRequest'{
    multipart_request = {_, Request}
}) ->
    #multipart_upload_request{
        multipart_request = from_protobuf(Request)
    };
from_protobuf(#'CreateMultipartUpload'{space_id = SpaceId, path = Path}) ->
    #create_multipart_upload{space_id = SpaceId, path = Path};
from_protobuf(#'UploadMultipartPart'{multipart_upload_id = UploadId, part = Part}) ->
    #upload_multipart_part{multipart_upload_id = UploadId, part = from_protobuf(Part)};
from_protobuf(#'ListMultipartParts'{
    multipart_upload_id = UploadId,
    limit = Limit,
    part_marker = PartMarker
}) ->
    #list_multipart_parts{
        multipart_upload_id = UploadId,
        limit = Limit,
        part_marker = PartMarker
    };
from_protobuf(#'AbortMultipartUpload'{multipart_upload_id = UploadId}) ->
    #abort_multipart_upload{multipart_upload_id = UploadId};
from_protobuf(#'CompleteMultipartUpload'{multipart_upload_id = UploadId}) ->
    #complete_multipart_upload{multipart_upload_id = UploadId};
from_protobuf(#'ListMultipartUploads'{
    space_id = SpaceId,
    limit = Limit,
    index_token = EncodedIndexToken
}) ->
    #list_multipart_uploads{
        space_id = SpaceId,
        limit = Limit,
        index_token = multipart_upload:decode_token(EncodedIndexToken)
    };

from_protobuf(#'MultipartParts'{
    parts = Parts,
    is_last = IsLast
}) ->
    #multipart_parts{
        parts = lists:map(fun(Part) -> from_protobuf(Part) end, Parts),
        is_last = IsLast
    };
from_protobuf(#'MultipartPart'{
    number = Number,
    size = Size,
    etag = Etag,
    last_modified = LastModified
}) ->
    #multipart_upload_part{
        number = Number,
        size = Size,
        etag = Etag,
        last_modified = LastModified
    };
from_protobuf(#'MultipartUpload'{
    multipart_upload_id = UploadId,
    path = Path,
    creation_time = CreationTime
}) ->
    #multipart_upload{
        multipart_upload_id = UploadId,
        path = Path,
        creation_time = CreationTime
    };
from_protobuf(#'MultipartUploads'{
    uploads = Uploads,
    is_last = IsLast,
    next_page_token = EncodedNextPageToken
}) ->
    {multipart_uploads, #multipart_uploads{
        uploads = lists:map(fun(Upload) -> from_protobuf(Upload) end, Uploads),
        is_last = IsLast,
        next_page_token = multipart_upload:decode_token(EncodedNextPageToken)
    }};

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#create_multipart_upload{space_id = SpaceId, path = Path}) ->
    {create_multipart_upload, #'CreateMultipartUpload'{space_id = SpaceId, path = Path}};
to_protobuf(#upload_multipart_part{multipart_upload_id = UploadId, part = Part}) ->
    {upload_multipart_part, #'UploadMultipartPart'{multipart_upload_id = UploadId, part = to_protobuf(Part)}};
to_protobuf(#list_multipart_parts{
    multipart_upload_id = UploadId, 
    limit = Limit, 
    part_marker = PartMarker
}) ->
    {list_multipart_parts, #'ListMultipartParts'{
        multipart_upload_id = UploadId, 
        limit = Limit, 
        part_marker = PartMarker
    }};
to_protobuf(#abort_multipart_upload{multipart_upload_id = UploadId}) ->
    {abort_multipart_upload, #'AbortMultipartUpload'{multipart_upload_id = UploadId}};
to_protobuf(#complete_multipart_upload{multipart_upload_id = UploadId}) ->
    {complete_multipart_upload, #'CompleteMultipartUpload'{multipart_upload_id = UploadId}};
to_protobuf(#list_multipart_uploads{
    space_id = SpaceId,
    limit = Limit,
    index_token = IndexToken
}) ->
    {list_multipart_uploads, #'ListMultipartUploads'{
        space_id = SpaceId,
        limit = Limit,
        index_token = multipart_upload:encode_token(IndexToken)
    }};
to_protobuf(#multipart_upload_request{
    multipart_request = Request
}) ->
    {multipart_upload_request, #'MultipartUploadRequest'{
        multipart_request = to_protobuf(Request)
    }};
to_protobuf(#multipart_upload_part{
    number = Number,
    size = Size,
    etag = Etag,
    last_modified = LastModified
}) ->
    #'MultipartPart'{
        number = Number,
        size = Size,
        etag = Etag,
        last_modified = LastModified
    };
to_protobuf(#multipart_parts{
    parts = Parts,
    is_last = IsLast
}) ->
    {multipart_parts, #'MultipartParts'{
        parts = lists:map(fun(Part) -> to_protobuf(Part) end, Parts),
        is_last = IsLast
    }};
to_protobuf(#multipart_upload{
    multipart_upload_id = UploadId,
    path = Path,
    creation_time = CreationTime
}) ->
    {multipart_upload, #'MultipartUpload'{
        multipart_upload_id = UploadId,
        path = Path,
        creation_time = CreationTime
    }};
to_protobuf(#multipart_uploads{
    uploads = Uploads,
    is_last = IsLast,
    next_page_token = NextPageToken
}) ->
    {multipart_uploads, #'MultipartUploads'{
        uploads = lists:map(fun(Upload) -> 
            {_, Record} = to_protobuf(Upload),
            Record
        end, Uploads),
        is_last = IsLast,
        next_page_token = multipart_upload:encode_token(NextPageToken)
    }};


%% OTHER
to_protobuf(undefined) -> undefined.
