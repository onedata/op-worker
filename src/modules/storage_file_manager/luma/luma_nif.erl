%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc LUMA nif wrapper
%%% @end
%%%-------------------------------------------------------------------
-module(luma_nif).
-author("Michal Wrona").

-include("global_definitions.hrl").

%% API
-export([init/0, create_ceph_user/6]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates new user in Ceph
%% @end
%%--------------------------------------------------------------------
-spec create_ceph_user(binary(), binary(), binary(), binary(), binary(), binary())
        -> {ok, {binary(), binary()}} | {error, binary()}.
create_ceph_user(_UserId, _MonHost, _ClusterName, _PoolName, _CephAdmin, _CephAdminKey) ->
    erlang:nif_error(luma_nif_not_loaded).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initialization function for the module.
%% Loads the NIF native library. The library is first searched for
%% in application priv dir, and then under ../priv and ./priv .
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok | {error, Reason :: term()}.
init() ->
    LibName = "luma_nif",
    LibPath =
        case code:priv_dir(?APP_NAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true ->
                        filename:join(["..", priv, LibName]);
                    _ ->
                        filename:join([priv, LibName])
                end;

            Dir ->
                filename:join(Dir, LibName)
        end,

    case erlang:load_nif(LibPath, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.
