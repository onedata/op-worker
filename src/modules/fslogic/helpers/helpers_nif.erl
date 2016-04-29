%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helpers NIF wrapper. Helper methods calls are asynchronous. Response can be received later
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("ctool/include/logging.hrl").

-type resource_handle() :: term().
-type open_mode() :: 'O_RDONLY' | 'O_WRONLY' | 'O_RDWR'. %% Exactly one of those
-type flag() :: open_mode() | 'O_NONBLOCK' | 'O_APPEND' | 'O_ASYNC' | 'O_FSYNC' | 'O_NOFOLLOW' | 'O_CREAT' | 'O_TRUNC' | 'O_EXCL'. %% Any of those
-type file_type() :: 'S_IFREG' | 'S_IFCHR' | 'S_IFBLK' | 'S_IFIFO' | 'S_IFSOCK'.
-type nif_string() :: string() | binary().
-type helper_args() :: #{binary() => binary()}.
-type request_id() :: {integer(), integer(), integer()}. %% Response message from helper will be {request_id(), Result :: term()}

-export_type([nif_string/0, resource_handle/0, file_type/0, helper_args/0]).

%% API
-export([init/0, set_threads_number/1]).
-export([new_helper_obj/2, new_helper_ctx/1, set_user_ctx/2, get_user_ctx/1]).
-export([username_to_uid/1, groupname_to_gid/1]).
-export([getattr/3, access/4, mknod/6, mkdir/4, unlink/3, rmdir/3, symlink/4, rename/4, link/4, chmod/4, chown/5]).
-export([truncate/4, open/4, read/5, write/5, release/3, flush/3, fsync/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Sets number of threads for IO service associated with helper.
%% @end
%%--------------------------------------------------------------------
-spec set_threads_number(#{HelperName :: nif_string() => Threads :: non_neg_integer()}) -> ok.
set_threads_number(_ThreadsByHelper) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Creates new helper object. Returned handle is only valid within local Erlang-VM.
%% @end
%%--------------------------------------------------------------------
-spec new_helper_obj(HelperName :: nif_string(), HelperArgs :: helper_args()) ->
    {ok, HelperObj :: resource_handle()} | {error, invalid_helper}.
new_helper_obj(_HelperName, _HelperArgs) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Creates new helper context object. Returned handle is only valid within local Erlang-VM.
%% @end
%%--------------------------------------------------------------------
-spec new_helper_ctx(HelperObj :: resource_handle()) -> {ok, HelperCTX :: resource_handle()}.
new_helper_ctx(_HelperObj) ->
    erlang:nif_error(helpers_nif_not_loaded).


%%--------------------------------------------------------------------
%% @doc Sets FS UID / FS GID in given helper context.
%% @end
%%--------------------------------------------------------------------
-spec set_user_ctx(HelperCTX :: resource_handle(), UserCTX :: helpers:user_ctx_map()) -> ok.
set_user_ctx(_HelperCTX, _UserCTX) ->
    erlang:nif_error(helpers_nif_not_loaded).


%%--------------------------------------------------------------------
%% @doc Gets current FS UID / FS GID from given helper context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_ctx(HelperCTX :: resource_handle()) -> {ok, UserCTX :: helpers:user_ctx_map()}.
get_user_ctx(_HelperCTX) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Transalates given username to UID.
%% @end
%%--------------------------------------------------------------------
-spec username_to_uid(UName :: string() | binary()) -> {ok, UID :: integer()} | {error, einval}.
username_to_uid(_UName) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Translates given groupname to GID.
%% @end
%%--------------------------------------------------------------------
-spec groupname_to_gid(GName :: string() | binary()) -> {ok, GID :: integer()} | {error, einval}.
groupname_to_gid(_GName) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec getattr(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
getattr(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec access(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Mask :: non_neg_integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
access(_HelperInstance, _HelperCTX, _File, _Mask) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec mknod(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(),
    Mode :: non_neg_integer(), Flags :: [flag()], Dev :: integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
mknod(_HelperInstance, _HelperCTX, _File, _Mode, _Flags, _Dev) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Mode :: non_neg_integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
mkdir(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec unlink(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
unlink(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
rmdir(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec symlink(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), From :: helpers:file(), To :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
symlink(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec rename(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), From :: helpers:file(), To :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
rename(_HelperInstance, _HelperCTX, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec link(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), From :: helpers:file(), To :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
link(_HelperInstance, _HelperCTX, _From, _To) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec chmod(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Mode :: non_neg_integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
chmod(_HelperInstance, _HelperCTX, _File, _Mode) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec chown(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), UID :: integer(), GID :: integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
chown(_HelperInstance, _HelperCTX, _File, _UID, _GID) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec truncate(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Size :: non_neg_integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
truncate(_HelperInstance, _HelperCTX, _File, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec open(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(),
    File :: helpers:file(), Flags :: [flag()]) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
open(_HelperInstance, _HelperCTX, _File, _Flags) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec read(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
read(_HelperInstance, _HelperCTX, _File, _Offset, _Size) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec write(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
write(_HelperInstance, _HelperCTX, _File, _Offset, _Data) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec release(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
release(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec flush(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
flush(_HelperInstance, _HelperCTX, _File) ->
    erlang:nif_error(helpers_nif_not_loaded).

%%--------------------------------------------------------------------
%% @doc Helper NIF method call. First argument shall be helper object from new_helper_obj/2. Second argument
%%      shall be context object from new_helper_ctx/0. All other arguments are described in corresponding helper documentacion.
%% @end
%%--------------------------------------------------------------------
-spec fsync(HelperInstance :: resource_handle(), HelperCTX :: resource_handle(), File :: helpers:file(), IsDataSync :: integer()) ->
    {ok, request_id()} | {error, Reason :: helpers:error_code()}.
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
-spec init() -> ok | {error, Reason :: term()}.
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

    case erlang:load_nif(LibPath, 0) of
        ok ->
            set_threads_number(),
            ok;
        {error, {reload, _}} ->
            set_threads_number(),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets default number of threads for each storage helper.
%% @end
%%--------------------------------------------------------------------
-spec set_threads_number() -> ok.
set_threads_number() ->
    {ok, CephThreads} = application:get_env(?APP_NAME, ceph_helper_threads_number),
    {ok, DioThreads} = application:get_env(?APP_NAME, direct_io_helper_threads_number),
    {ok, S3Threads} = application:get_env(?APP_NAME, s3_helper_threads_number),
    set_threads_number(#{
        ?CEPH_HELPER_NAME => CephThreads,
        ?DIRECTIO_HELPER_NAME => DioThreads,
        ?S3_HELPER_NAME => S3Threads
    }).
