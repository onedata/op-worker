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

create_ceph_user(_User_id, _Mon_host, _Cluster_name, _Pool_name, _Ceph_admin, _Ceph_admin_key) ->
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
-spec init() -> ok | {error, Reason :: atom()}.
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

    erlang:load_nif(LibPath, 0).
