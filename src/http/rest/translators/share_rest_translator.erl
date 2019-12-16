%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% share entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(share_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = shared_dir}, _, value, ShareId) ->
    PathTokens = [<<"shares-id">>, ShareId],
    ?CREATED_REPLY(PathTokens, #{<<"shareId">> => ShareId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, ShareData0) ->
    {RootFileGuid, ShareData1} = maps:take(<<"rootFile">>, ShareData0),
    {ok, ObjectId} = file_id:guid_to_objectid(RootFileGuid),
    ?OK_REPLY(ShareData1#{<<"rootFileId">> => ObjectId}).
