%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the share model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(share_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/api_errors.hrl").


%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
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
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(ModelType, ShareId) ->
    case ModelType of
        <<"share">> ->
            SessionId = gui_session:get_session_id(),
            share_record(SessionId, ShareId);
        <<"share-public">> ->
            public_share_record(ShareId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"share-public">>) ->
    gui_error:report_error(<<"Not implemented">>);
find_all(<<"share">>) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(_, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(_, _Data) ->
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
update_record(<<"share-public">>, _ShareId, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
update_record(<<"share">>, ShareId, [{<<"name">>, Name}]) ->
    SessionId = gui_session:get_session_id(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set share name to empty string.">>);
        NewName ->
            case share_logic:update_name(SessionId, ShareId, NewName) of
                ok ->
                    % Push container dir as its name has also changed.
                    {ok, FileData} = file_data_backend:find_record(
                        <<"file-shared">>, <<"containerDir.", ShareId/binary>>
                    ),
                    FileDataNewName = lists:keyreplace(
                        <<"name">>, 1, FileData, {<<"name">>, NewName}
                    ),
                    gui_async:push_updated(<<"file-shared">>, FileDataNewName),
                    ok;
                {error, {403, <<>>, <<>>}} ->
                    gui_error:report_warning(<<"You do not have permissions to "
                    "manage shares in this space.">>);
                _ ->
                    gui_error:report_warning(
                        <<"Cannot change share name due to unknown error.">>)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"share-public">>, _ShareId) ->
    gui_error:report_error(<<"Not implemented">>);
delete_record(<<"share">>, ShareId) ->
    SessionId = gui_session:get_session_id(),
    case logical_file_manager:remove_share(SessionId, ShareId) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(<<"You do not have permissions to "
            "manage shares in this space.">>);
        _ ->
            gui_error:report_warning(
                <<"Cannot remove share due to unknown error.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Constructs a share record for given ShareId.
%% @end
%%--------------------------------------------------------------------
-spec share_record(SessionId :: session:id(), ShareId :: od_share:id()) ->
    {ok, proplists:proplist()}.
share_record(SessionId, ShareId) ->
    case share_logic:get(SessionId, ShareId) of
        {error, _} ->
            gui_error:unauthorized();
        {ok, #document{value = ShareRecord}} ->
            #od_share{
                name = Name,
                root_file = RootFileId,
                space = SpaceId,
                public_url = PublicURL,
                handle = HandleId
            } = ShareRecord,
            {ok, [
                {<<"id">>, ShareId},
                {<<"name">>, Name},
                {<<"file">>, fslogic_uuid:share_guid_to_guid(RootFileId)},
                {<<"containerDir">>, <<"containerDir.", ShareId/binary>>},
                {<<"dataSpace">>, SpaceId},
                {<<"publicUrl">>, PublicURL},
                {<<"handle">>, gs_protocol:undefined_to_null(HandleId)},
                {<<"user">>, gui_session:get_user_id()}
            ]}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Constructs a public share record for given ShareId.
%% @end
%%--------------------------------------------------------------------
-spec public_share_record(ShareId :: od_share:id()) -> {ok, proplists:proplist()}.
public_share_record(ShareId) ->
    case share_logic:get_public_data(?GUEST_SESS_ID, ShareId) of
        {error, _} ->
            gui_error:unauthorized();
        {ok, #document{value = ShareRecord}} ->
            #od_share{
                name = Name,
                root_file = RootFileId,
                public_url = PublicURL,
                handle = HandleId
            } = ShareRecord,
            {ok, [
                {<<"id">>, ShareId},
                {<<"name">>, Name},
                {<<"file">>, RootFileId},
                {<<"containerDir">>, <<"containerDir.", ShareId/binary>>},
                {<<"publicUrl">>, PublicURL},
                {<<"handle">>, gs_protocol:undefined_to_null(HandleId)}
            ]}
    end.