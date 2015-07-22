%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif).
-author("Rafal Slota").
-on_load(init/0).

-include("global_definitions.hrl").

%% API
-export([new_helper_obj/2, new_helper_ctx/0, set_user_ctx/3, get_user_ctx/1]).
-export([mkdir/4]).
-export([load/1]).

%%%===================================================================
%%% API
%%%===================================================================

new_helper_obj(_HelperName, _HelperArgs) ->
    erlang:nif_error(helpers_nif_not_loaded).

new_helper_ctx() ->
    erlang:nif_error(helpers_nif_not_loaded).

set_user_ctx(HelperCTX, User, Group) ->
    erlang:nif_error(helpers_nif_not_loaded).

get_user_ctx(HelperCTX) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec mkdir(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Mode :: non_neg_integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
mkdir(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

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
    LibName = "helpers_nif",
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

load(LibPath) ->
    erlang:load_nif(LibPath, 0).