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

-type resource_handle() :: term().

%% API
-export([new_helper_obj/2, new_helper_ctx/0, set_user_ctx/3, get_user_ctx/1]).
-export([username_to_uid/1, groupname_to_gid/1]).
-export([getattr/3, access/4, mknod/5, mkdir/4, unlink/3, rmdir/3, symlink/4, rename/4, link/4, chmod/4, chown/5]).
-export([truncate/4, open/3, read/5, write/5, release/3, flush/3, fsync/4]).
-export([load/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec new_helper_obj(HelperName :: string() | binary(), HelperArgs :: [string() | binary()]) ->
    {ok, HelperObj :: resource_handle()} | {error, invalid_helper}.
new_helper_obj(_HelperName, _HelperArgs) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec new_helper_ctx() -> {ok, HelperCTX :: resource_handle()}.
new_helper_ctx() ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec set_user_ctx(HelperCTX :: resource_handle(), User :: integer(), Group :: integer()) -> ok.
set_user_ctx(_HelperCTX, _User, _Group) ->
    erlang:nif_error(helpers_nif_not_loaded).

get_user_ctx(_HelperCTX) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec username_to_uid(UName :: string() | binary()) -> {ok, UID :: integer()}.
username_to_uid(_UName) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec groupname_to_gid(GName :: string() | binary()) -> {ok, GID :: integer()}.
groupname_to_gid(_GName) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec getattr(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
getattr(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec access(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Mask :: non_neg_integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
access(_HelperInstance, _HelperCTX, _File, _Mask) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec mknod(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Mode :: non_neg_integer(), _Dev :: integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
mknod(_HelperInstance, _HelperCTX, _File, _Mode, _Dev) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec mkdir(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Mode :: non_neg_integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
mkdir(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec unlink(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
unlink(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec rmdir(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
rmdir(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec symlink(HelperInstance :: term(), HelperCTX :: term(), From :: helpers:file(), To :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
symlink(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec rename(HelperInstance :: term(), HelperCTX :: term(), From :: helpers:file(), To :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
rename(_HelperInstance, _HelperCTX, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec link(HelperInstance :: term(), HelperCTX :: term(), From :: helpers:file(), To :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
link(_HelperInstance, _HelperCTX, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec chmod(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Mode :: non_neg_integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
chmod(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec chown(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), UID :: integer(), GID :: integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
chown(_HelperInstance, _HelperCTX, _File, _UID, _GID) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec truncate(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Size :: non_neg_integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
truncate(_HelperInstance, _HelperCTX, _File, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec open(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    {ok, FD :: term()} | {error, Reason :: helpers:error_code()}.
open(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec read(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | {error, Reason :: helpers:error_code()}.
read(_HelperInstance, _HelperCTX, _File, _Offset, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec write(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, Size :: non_neg_integer()} | {error, Reason :: helpers:error_code()}.
write(_HelperInstance, _HelperCTX, _File, _Offset, _Data) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec release(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
release(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec flush(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file()) ->
    ok | {error, Reason :: helpers:error_code()}.
flush(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

-spec fsync(HelperInstance :: term(), HelperCTX :: term(), File :: helpers:file(), IsDataSync :: integer()) ->
    ok | {error, Reason :: helpers:error_code()}.
fsync(_HelperInstance, _HelperCTX, _File, _IsDataSync) ->
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