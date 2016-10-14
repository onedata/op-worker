%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(public_share_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"share-public">>, ShareId) ->
    {ok, #document{
        value = #od_share{
            name = Name,
            root_file = RootFileId,
            public_url = PublicURL,
            handle = Handle
        }}} = share_logic:get(?GUEST_SESS_ID, ShareId),
    HandleVal = case Handle of
        undefined -> null;
        <<"undefined">> -> null;
        _ -> Handle
    end,
    {ok, [
        {<<"id">>, ShareId},
        {<<"name">>, Name},
        {<<"file">>, RootFileId},
        {<<"containerDir">>, <<"containerDir.", ShareId/binary>>},
        {<<"publicUrl">>, PublicURL},
        {<<"handle">>, HandleVal}
    ]};

find(<<"file-property-public">>, AssocId) ->
    SessionId = ?GUEST_SESS_ID,
    {_, FileId} = op_gui_utils:association_to_ids(AssocId),
    {ok, XattrKeys} = logical_file_manager:list_xattr(
        SessionId, {guid, FileId}, false, false
    ),
    Basic = lists:map(
        fun(Key) ->
            {ok, #xattr{value = Value}} = logical_file_manager:get_xattr(
                SessionId, {guid, FileId}, Key, false
            ),
            {Key, Value}
        end, XattrKeys),
    BasicVal = case Basic of
        [] -> null;
        _ -> Basic
    end,
    GetJSONResult = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, json, [], false
    ),
    JSONVal = case GetJSONResult of
        {error, ?ENOATTR} -> null;
        {ok, Map} when map_size(Map) =:= 0 -> <<"{}">>;
        {ok, JSON} -> json_utils:decode(json_utils:encode_map(JSON))
    end,
    GetRDFResult = logical_file_manager:get_metadata(
        SessionId, {guid, FileId}, rdf, [], false
    ),
    RDFVal = case GetRDFResult of
        {error, ?ENOATTR} -> null;
        {ok, RDF} -> RDF
    end,
    {ok, [
        {<<"id">>, AssocId},
        {<<"file">>, AssocId},
        {<<"basic">>, BasicVal},
        {<<"json">>, JSONVal},
        {<<"rdf">>, RDFVal}
    ]};

find(<<"handle-public">>, HandleId) ->
    {ok, #document{
        value = #od_handle{
            public_handle = PublicHandle,
            metadata = Metadata
        }}} = handle_logic:get(?GUEST_SESS_ID, HandleId),
    {ok, [
        {<<"id">>, HandleId},
        {<<"metadataString">>, Metadata},
        {<<"publicHandle">>, PublicHandle}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(_) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(_, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(_, _Id) ->
    gui_error:report_error(<<"Not implemented">>).
