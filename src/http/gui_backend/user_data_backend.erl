%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(user_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([user_record/2]).

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
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"user">>, UserId) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case gui_session:get_user_id() of
        UserId ->
            {ok, user_record(UserAuth, UserId)};
        _ ->
            gui_error:unauthorized()
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"user">>) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"user">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"user">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"user">>, UserId, Data) ->
    % FIXME: validate UserId
    case Data of
        [{<<"defaultSpaceId">>, DefaultSpace}] ->
            UserAuth = op_gui_utils:get_user_auth(),
            case user_logic:set_default_space(UserAuth, DefaultSpace) of
                ok ->
                    ok;
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change default space due to unknown error.">>)
            end;
        _ ->
            gui_error:report_error(<<"Not implemented">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"user">>, _Id) ->
    gui_error:report_error(<<"Not implemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant user record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec user_record(Auth :: #token_auth{}, UserId :: od_user:id()) ->
    proplists:proplist().
user_record(Auth, UserId) ->
    {ok, #document{value = #od_user{
        name = Name,
        default_space = DefaultSpaceValue
    }}} = od_user:get(UserId),
    DefaultSpace = case DefaultSpaceValue of
        undefined -> null;
        _ -> DefaultSpaceValue
    end,
    Groups = op_gui_utils:find_all_groups(Auth, UserId),
    Spaces = op_gui_utils:find_all_spaces(Auth, UserId),
    Shares = lists:foldl(
        fun(SpaceId, Acc) ->
            % Make sure that user is allowed to view shares in this space
            Authorized = space_logic:has_effective_privilege(
                SpaceId, UserId, space_view_data
            ),
            case Authorized of
                true ->
                    {ok, #document{value = #od_space{
                        shares = ShareIds
                    }}} = od_space:get(SpaceId),
                    ShareIds ++ Acc;
                false ->
                    Acc
            end
        end, [], Spaces),
    {ok, HandleServices} = user_logic:get_effective_handle_services(
        Auth, UserId
    ),
    [
        {<<"id">>, UserId},
        {<<"name">>, Name},
        {<<"defaultSpaceId">>, DefaultSpace},
        {<<"groups">>, Groups},
        {<<"spaces">>, Spaces},
        {<<"shares">>, Shares},
        {<<"handleServices">>, HandleServices}
    ].
