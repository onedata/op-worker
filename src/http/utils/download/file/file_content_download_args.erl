%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `download_args` interface for
%%% #file_content_download_args{}.
%%% @end
%%%-------------------------------------------------------------------
-module(file_content_download_args).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("http/http_download.hrl").

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #file_content_download_args{}.

-export_type([record/0]).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#file_content_download_args{
    session_id = SessionId,
    file_guids = FileGuids,
    follow_symlinks = FollowSymlinks
}, _NestedRecordEncoder) ->
    #{
        <<"sessionId">> => SessionId,
        <<"fileGuids">> => FileGuids,
        <<"followSymlinks">> => FollowSymlinks
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"sessionId">> := SessionId,
    <<"fileGuids">> := FileGuids,
    <<"followSymlinks">> := FollowSymlinks
}, _NestedRecordDecoder) ->
    #file_content_download_args{
        session_id = SessionId,
        file_guids = FileGuids,
        follow_symlinks = FollowSymlinks
    }.
