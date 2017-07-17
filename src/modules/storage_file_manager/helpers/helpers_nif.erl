%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface to the helpers NIF library.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").

-type helper_handle() :: term().
-type file_handle() :: term().
-type file_type() :: 'S_IFREG' | 'S_IFCHR' | 'S_IFBLK' | 'S_IFIFO' | 'S_IFSOCK'.
-type open_flag() :: 'O_WRONLY' | 'O_RDONLY' | 'O_RDWR'.
-type response_ref() :: {integer(), integer(), integer()}.
-type response() :: {response_ref(), Result :: term()}.

-export_type([helper_handle/0, file_handle/0, response_ref/0, response/0]).

%% API
-export([init/0]).
-export([get_handle/2]).
-export([getattr/2, access/3, mknod/5, mkdir/3, unlink/2, rmdir/2, symlink/3,
    rename/3, link/3, chmod/3, chown/4, truncate/3, open/3, read/3, write/3,
    release/1, flush/1, fsync/2, readdir/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new helper instance and returns a handle.
%% IMPORTANT! Helper handle is valid only within local Erlang VM.
%% @end
%%--------------------------------------------------------------------
-spec get_handle(helpers:name(), helpers:args()) ->
    {ok, helper_handle()} | {error, Reason :: term()}.
get_handle(_Name, _Params) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec readdir(helper_handle(), helpers:file_id(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) -> {ok, response_ref()} | {error, Reason :: term()}.
readdir(_Handle, _FileId, _Offset, _Count) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec getattr(helper_handle(), helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
getattr(_Handle, _FileId) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec access(helper_handle(), helpers:file_id(), Mask :: non_neg_integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
access(_Handle, _FileId, _Mask) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec mknod(helper_handle(), helpers:file_id(), Mode :: non_neg_integer(),
    Flags :: [file_type()], Dev :: integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
mknod(_Handle, _FileId, _Mode, _Flags, _Dev) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec mkdir(helper_handle(), helpers:file_id(), Mode :: non_neg_integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
mkdir(_Handle, _FileId, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec unlink(helper_handle(), helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
unlink(_Handle, _FileId) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec rmdir(helper_handle(), helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
rmdir(_Handle, _FileId) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec symlink(helper_handle(), From :: helpers:file_id(), To :: helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
symlink(_Handle, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec rename(helper_handle(), From :: helpers:file_id(), To :: helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
rename(_Handle, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec link(helper_handle(), From :: helpers:file_id(), To :: helpers:file_id()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
link(_Handle, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec chmod(helper_handle(), helpers:file_id(), Mode :: non_neg_integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
chmod(_Handle, _FileId, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec chown(helper_handle(), helpers:file_id(), posix_user:uid(), posix_user:gid()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
chown(_Handle, _FileId, _Uid, _Gid) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec truncate(helper_handle(), helpers:file_id(), Size :: non_neg_integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
truncate(_Handle, _FileId, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec open(helper_handle(), helpers:file_id(), Flags :: [open_flag()]) ->
    {ok, response_ref()} | {error, Reason :: term()}.
open(_Handle, _FileId, _Flags) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec read(file_handle(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
read(_Handle, _Offset, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec write(file_handle(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
write(_Handle, _Offset, _Data) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec release(file_handle()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
release(_Handle) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec flush(file_handle()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
flush(_Handle) ->
    erlang:nif_error(helpers_nif_not_loaded).


-spec fsync(file_handle(), IsDataSync :: integer()) ->
    {ok, response_ref()} | {error, Reason :: term()}.
fsync(_Handle, _IsDataSync) ->
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
-spec init() -> ok | {error, Reason :: term()}.
init() ->
    LibName = "helpers_nif",
    LibPath = case code:priv_dir(?APP_NAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true -> filename:join(["..", priv, LibName]);
                _ -> filename:join([priv, LibName])
            end;
        Dir ->
            filename:join(Dir, LibName)
    end,

    case erlang:load_nif(LibPath, prepare_args()) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns NIF library initialization arguments.
%% @end
%%--------------------------------------------------------------------
-spec prepare_args() -> #{binary() => binary()}.
prepare_args() ->
    lists:foldl(fun(EnvKey, Map) ->
        {ok, EnvValue} = application:get_env(?APP_NAME, EnvKey),
        maps:put(str_utils:to_binary(EnvKey), str_utils:to_binary(EnvValue), Map)
    end, #{}, [
        ceph_helper_threads_number,
        posix_helper_threads_number,
        s3_helper_threads_number,
        swift_helper_threads_number,
        glusterfs_helper_threads_number,
        buffer_helpers,
        buffer_scheduler_threads_number,
        read_buffer_min_size,
        read_buffer_max_size,
        read_buffer_prefetch_duration,
        write_buffer_min_size,
        write_buffer_max_size,
        write_buffer_flush_delay
    ]).
