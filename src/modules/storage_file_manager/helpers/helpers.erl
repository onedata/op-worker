%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Wrapper for helpers_nif module. Calls its methods in synchronous manner (call + response receive).
%%% @end
%%%-------------------------------------------------------------------
-module(helpers).
-author("Rafal Slota").

-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([name/1, args/1, new_handle/2, new_handle/3, extract_args/1]).
-export([getattr/2, access/3, mknod/4, mkdir/3, unlink/2, rmdir/2, symlink/3, rename/3, link/3, chmod/3]).
-export([chown/4, truncate/3, open/3, read/3, write/3, release/1, flush/1, fsync/2, readdir/4]).

%% Internal context record.
-record(helper_handle, {
    instance :: helpers_nif:resource_handle(),
    timeout :: timeout(),
    helper_name :: name()
}).

%% Internal context record.
-record(helper_file_handle, {
    instance :: helpers_nif:resource_handle(),
    timeout :: timeout()
}).

-define(extract(Record, What),
    case Record of
        #helper_handle{What = W} -> W;
        #helper_file_handle{What = W} -> W
    end).

-define(do_apply_helper_nif(Handle, Method, Args),
    apply_helper_nif(?extract(Handle, instance), ?extract(Handle, timeout), Method, Args)).

-type file() :: binary().
-type open_flag() :: fslogic_worker:open_flag().
-type error_code() :: atom().
-type handle() :: #helper_handle{}.
-type file_handle() :: #helper_file_handle{}.
-type name() :: binary().
-type args() :: helpers_nif:helper_args().
-type init() :: #helper_init{}.

-export_type([file/0, open_flag/0, error_code/0, handle/0, file_handle/0,
    name/0, args/0, init/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns helper name.
%% @end
%%--------------------------------------------------------------------
-spec name(Helper :: init() | handle()) -> Name :: name().
name(#helper_init{name = Name}) -> Name;
name(#helper_handle{helper_name = Name}) -> Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns helper arguments map.
%% @end
%%--------------------------------------------------------------------
-spec args(Helper :: init()) -> Args :: args().
args(#helper_init{args = Args}) ->
    Args.

%%--------------------------------------------------------------------
%% @doc Creates new helper object along with helper context object. Valid within local Erlang-VM.
%%      @todo: maybe cache new_helper_obj result
%% @end
%%--------------------------------------------------------------------
-spec new_handle(init(), helpers_user:ctx()) -> handle().
new_handle(#helper_init{name = Name, args = Args}, UserCtx) ->
    new_handle(Name, Args, UserCtx).


%%--------------------------------------------------------------------
%% @doc Creates new helper object along with helper context object. Valid within local Erlang-VM.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(HelperName :: helpers_nif:nif_string(), args(), helpers_user:ctx()) -> handle().
new_handle(HelperName, HelperArgs, UserCtx) ->
    ?debug("helpers:new_handle~p ~p ~p", [HelperName, HelperArgs, UserCtx]),
    CtxArgs = extract_args(UserCtx),
    {ok, HelperObj} = helpers_nif:new_helper_obj(HelperName, maps:merge(HelperArgs, CtxArgs)),
    #helper_handle{
        instance = HelperObj,
        helper_name = HelperName,
        timeout = helpers_utils:get_timeout(HelperArgs)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Translates user context record to a map of arguments for helper construction.
%% @end
%%--------------------------------------------------------------------
-spec extract_args(helpers_user:ctx()) -> args().
extract_args(#ceph_user_ctx{user_name = UserName, user_key = UserKey}) ->
    #{<<"user_name">> => UserName, <<"key">> => UserKey};
extract_args(#posix_user_ctx{uid = UID, gid = GID}) ->
    #{<<"uid">> => integer_to_binary(UID), <<"gid">> => integer_to_binary(GID)};
extract_args(#s3_user_ctx{access_key = AccessKey, secret_key = SecretKey}) ->
    #{<<"access_key">> => AccessKey, <<"secret_key">> => SecretKey};
extract_args(#swift_user_ctx{user_name = UserName, password = Password}) ->
    #{<<"user_name">> => UserName, <<"password">> => Password};
extract_args(Unknown) ->
    ?error("Unknown user context ~p", [Unknown]),
    erlang:error({unknown_user_ctx_type, erlang:element(1, Unknown)}).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec readdir(handle(), File :: file(), Offset :: non_neg_integer(), Count :: non_neg_integer()) ->
    {ok, [file()]} | {error, term()}.
readdir(#helper_handle{} = HelperHandle, File, Offset, Count) ->
    apply_helper_nif(HelperHandle, readdir, [File, Offset, Count]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec getattr(handle(), File :: file()) -> {ok, #statbuf{}} | {error, term()}.
getattr(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, getattr, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec access(handle(), File :: file(), Mask :: non_neg_integer()) -> ok | {error, term()}.
access(#helper_handle{} = HelperHandle, File, Mask) ->
    apply_helper_nif(HelperHandle, access, [File, Mask]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mknod(handle(), File :: file(), Mode :: non_neg_integer(), Type :: atom()) -> ok | {error, term()}.
mknod(#helper_handle{} = HelperHandle, File, Mode, Type) ->
    apply_helper_nif(HelperHandle, mknod, [File, Mode, file_type_for_nif(Type), 0]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
mkdir(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mkdir, [File, Mode]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle(), File :: file()) -> ok | {error, term()}.
unlink(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, unlink, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle(), File :: file()) -> ok | {error, term()}.
rmdir(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, rmdir, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec symlink(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
symlink(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, symlink, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rename(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
rename(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, rename, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec link(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
link(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, link, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
chmod(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, chmod, [File, Mode]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chown(handle(), File :: file(), UID :: non_neg_integer() | -1, GID :: non_neg_integer() | -1) -> ok | {error, term()}.
chown(#helper_handle{} = HelperHandle, File, UID, GID) ->
    apply_helper_nif(HelperHandle, chown, [File, UID, GID]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), File :: file(), Size :: non_neg_integer()) -> ok | {error, term()}.
truncate(#helper_handle{} = HelperHandle, File, Size) ->
    apply_helper_nif(HelperHandle, truncate, [File, Size]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be handle() from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), File :: file(), Flag :: open_flag()) -> {ok, file_handle()} | {error, term()}.
open(#helper_handle{timeout = Timeout} = HelperHandle, File, Flag) ->
    case apply_helper_nif(HelperHandle, open, [File, open_flag_for_nif(Flag)]) of
        {ok, FH} -> {ok, #helper_file_handle{instance = FH, timeout = Timeout}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be file_handle() from open/3.
%% @end
%%--------------------------------------------------------------------
-spec read(file_handle(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | {error, term()}.
read(#helper_file_handle{} = HelperFileHandle, Offset, Size) ->
    apply_helper_nif(HelperFileHandle, read, [Offset, Size]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be file_handle() from open/3.
%% @end
%%--------------------------------------------------------------------
-spec write(file_handle(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, Size :: non_neg_integer()} | {error, term()}.
write(#helper_file_handle{} = HelperFileHandle, Offset, Data) ->
    apply_helper_nif(HelperFileHandle, write, [Offset, Data]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be file_handle() from open/3.
%% @end
%%--------------------------------------------------------------------
-spec release(file_handle()) -> ok | {error, term()}.
release(#helper_file_handle{} = HelperFileHandle) ->
    apply_helper_nif(HelperFileHandle, release, []).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be file_handle() from open/3.
%% @end
%%--------------------------------------------------------------------
-spec flush(file_handle()) -> ok | {error, term()}.
flush(#helper_file_handle{} = HelperFileHandle) ->
    apply_helper_nif(HelperFileHandle, flush, []).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be file_handle() from open/3.
%% @end
%%--------------------------------------------------------------------
-spec fsync(file_handle(), IsDataSync :: boolean()) -> ok | {error, term()}.
fsync(#helper_file_handle{} = HelperFileHandle, IsDataSync) ->
    apply_helper_nif(HelperFileHandle, fsync, [boolean_for_nif(IsDataSync)]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Calls given helpers_nif method with given args while inserting HelperInstance :: resource_handle() and
%%      HelperCTX :: resource_handle() to this arguments list (from given #helper_handle{}.
%%      After call, receives and returns response.
%% @end
%%--------------------------------------------------------------------
-spec apply_helper_nif(handle() | file_handle(), Method :: atom(), [term()]) -> ok | {ok, term()} | {error, term()}.
apply_helper_nif(#helper_handle{instance = Instance, timeout = Timeout}, Method, Args) ->
    apply_helper_nif(Instance, Timeout, Method, Args);
apply_helper_nif(#helper_file_handle{instance = Instance, timeout = Timeout}, Method, Args) ->
    apply_helper_nif(Instance, Timeout, Method, Args).

-spec apply_helper_nif(helpers_nif:resource_handle(), Timeout :: non_neg_integer(),
    Method :: atom(), [term()]) -> ok | {ok, term()} | {error, term()}.
apply_helper_nif(Instance, Timeout, Method, Args) ->
    {ok, Guard} = apply(helpers_nif, Method, [Instance | Args]),
    receive
        {Guard, Result} ->
            Result
    after Timeout ->
        {error, nif_timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a file type from helpers API to an argument for NIF
%% function.
%% @end
%%--------------------------------------------------------------------
-spec file_type_for_nif(atom()) -> [atom()].
file_type_for_nif(reg) -> ['S_IFREG'];
file_type_for_nif(chr) -> ['S_IFCHR'];
file_type_for_nif(blk) -> ['S_IFBLK'];
file_type_for_nif(fifo) -> ['S_IFIFO'];
file_type_for_nif(sock) -> ['S_IFSOCK'].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts an open flag from helpers API to an argument for NIF
%% function.
%% @end
%%--------------------------------------------------------------------
-spec open_flag_for_nif(open_flag()) -> [atom()].
open_flag_for_nif(write) -> ['O_WRONLY'];
open_flag_for_nif(read) -> ['O_RDONLY'];
open_flag_for_nif(rdwr) -> ['O_RDWR'].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a boolean to an argument for NIF function.
%% @end
%%--------------------------------------------------------------------
-spec boolean_for_nif(boolean()) -> 0 | 1.
boolean_for_nif(true) -> 1;
boolean_for_nif(false) -> 0.
