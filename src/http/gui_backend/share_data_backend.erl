%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(share_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([share_record/1]).

-export([add_share_mapping/2, get_share_mapping/1]).

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
find(<<"share">>, ShareId) ->
    {ok, share_record(ShareId)}.

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"share">>) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = g_session:get_user_id(),
    SpaceIds = op_gui_utils:find_all_spaces(UserAuth, UserId),
    % TODO check privileges
    ShareIds = lists:foldl(
        fun(SpaceId, Acc) ->
            {ok, #document{
                value = #space_info{
                    shares = Shares
                }}} = space_info:get(SpaceId),
            Shares ++ Acc
        end, [], SpaceIds),
    Res = lists:map(
        fun(ShareId) ->
            {ok, ShareData} = find(<<"share">>, ShareId),
            ShareData
        end, ShareIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"share">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"share">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"share">>, SpaceId, [{<<"isDefault">>, Flag}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case Flag of
        undefined ->
            ok;
        false ->
            ok;
        true ->
            case user_logic:set_default_space(UserAuth, SpaceId) of
                ok ->
                    ok;
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change default space due to unknown error.">>)
            end
    end;

update_record(<<"share">>, SpaceId, [{<<"name">>, Name}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set space name to empty string.">>);
        NewName ->
            case space_logic:set_name(UserAuth, SpaceId, NewName) of
                ok ->
                    ok;
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change space name due to unknown error.">>)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"share">>, SpaceId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:delete(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot remove space due to unknown error.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec share_record(ShareId :: binary()) -> proplists:proplist().
share_record(ShareId) ->
    CurrentUser = g_session:get_user_id(),
    UserAuth = op_gui_utils:get_user_auth(),
    {ok, #document{
        value = #share_info{
            name = Name,
            root_file_id = RootFileId,
            parent_space = ParentSpaceId,
            public_url = PublicURL
        }}} = space_logic:get(UserAuth, ShareId, CurrentUser),
    [
        {<<"id">>, ShareId},
        {<<"name">>, Name},
        {<<"file">>, RootFileId},
        {<<"dataSpace">>, ParentSpaceId},
        {<<"publicUrl">>, PublicURL}
    ].


add_share_mapping(FileId, ShareId) ->
    case share_info:exists(<<"magitrzny-rekord">>) of
        true ->
            ok;
        false ->
            share_info:create(#document{
                key = <<"magitrzny-rekord">>,
                value = #share_info{name = #{}}
            })
    end,
    {ok, _} = share_info:update(<<"magitrzny-rekord">>, fun(Space) ->
        #share_info{name = Mapping} = Space,
        NewMapping = maps:put(FileId, ShareId, Mapping),
        {ok, Space#share_info{name = NewMapping}}
    end).


get_share_mapping(FileId) ->
    case share_info:exists(<<"magitrzny-rekord">>) of
        true ->
            {ok, #document{
                value = #share_info{
                    name = Mapping
                }}} = share_info:get(<<"magitrzny-rekord">>),
            maps:get(FileId, Mapping, null);
        false ->
            null
    end.