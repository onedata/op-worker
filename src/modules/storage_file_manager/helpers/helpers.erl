%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides a synchronous interface to the helpers NIF library.
%%% It wraps {@link helpers_nif} module by calling its functions and awaiting
%%% results.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_helper_handle/2]).
-export([getattr/2, access/3, mknod/4, mkdir/3, unlink/3, rmdir/2, symlink/3,
    rename/3, link/3, chmod/3, chown/4, truncate/4, setxattr/6, getxattr/3,
    removexattr/3, listxattr/2, open/3, read/3, write/3, release/1, flush/1,
    fsync/2, readdir/4]).
-export([init_counters/0, init_report/0]).
%% For tests
-export([apply_helper_nif/3, receive_loop/2]).

-record(file_handle, {
    handle :: helpers_nif:file_handle(),
    timeout :: timeout()
}).

-type file_id() :: binary().
-type open_flag() :: rdwr | write | read.
-type file_type_flag() :: reg | chr | blk | fifo | sock.
-type helper() :: #helper{}.
-type helper_handle() :: #helper_handle{}.
-type file_handle() :: #file_handle{}.

-export_type([file_id/0, open_flag/0, helper/0, helper_handle/0, file_handle/0]).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, count, Param)).
-define(EXOMETER_TIME_NAME(Param), ?exometer_name(?MODULE, time,
    list_to_atom(atom_to_list(Param) ++ "_time"))).
-define(EXOMETER_COUNTERS, [get_helper_handle, readdir, getattr, access, mknod,
    mkdir, unlink, rmdir, symlink, rename, link, chmod, chown, truncate,
    setxattr, getxattr, removexattr, listxattr, open, read, write, release,
    flush, fsync]).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:get_helper/2} function and constructs helper handle
%% record.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_handle(helper(), helper:user_ctx()) -> helper_handle().
get_helper_handle(Helper, UserCtx) ->
    {ok, #helper{name = Name, args = Args}} = helper:set_user_ctx(Helper, UserCtx),
    {ok, Handle} = helpers_nif:get_handle(Name, Args),
    #helper_handle{
        handle = Handle,
        timeout = helper:get_timeout(Helper)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:readdir/4} function.
%% @end
%%--------------------------------------------------------------------
-spec readdir(helper_handle(), file_id(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) -> {ok, [file_id()]} | {error, Reason :: term()}.
readdir(Handle, FileId, Offset, Count) ->
    ?MODULE:apply_helper_nif(Handle, readdir, [FileId, Offset, Count]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:getattr/2} function.
%% @end
%%--------------------------------------------------------------------
-spec getattr(helper_handle(), file_id()) ->
    {ok, #statbuf{}} | {error, Reason :: term()}.
getattr(Handle, FileId) ->
    ?MODULE:apply_helper_nif(Handle, getattr, [FileId]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:access/3} function.
%% @end
%%--------------------------------------------------------------------
-spec access(helper_handle(), file_id(), Mask :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
access(Handle, FileId, Mask) ->
    ?MODULE:apply_helper_nif(Handle, access, [FileId, Mask]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:mknod/5} function.
%% @end
%%--------------------------------------------------------------------
-spec mknod(helper_handle(), file_id(), Mode :: non_neg_integer(), Type :: atom()) ->
    ok | {error, Reason :: term()}.
mknod(Handle, FileId, Mode, Type) ->
    ?MODULE:apply_helper_nif(Handle, mknod, [FileId, Mode, [file_type_for_nif(Type)], 0]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:mkdir/3} function.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(helper_handle(), file_id(), Mode :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
mkdir(Handle, FileId, Mode) ->
    ?MODULE:apply_helper_nif(Handle, mkdir, [FileId, Mode]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:unlink/2} function.
%% @end
%%--------------------------------------------------------------------
-spec unlink(helper_handle(), file_id(), CurrentSize :: non_neg_integer())
    -> ok | {error, Reason :: term()}.
unlink(Handle, FileId, CurrentSize) ->
    ?MODULE:apply_helper_nif(Handle, unlink, [FileId, CurrentSize]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:rmdir/2} function.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(helper_handle(), file_id()) -> ok | {error, Reason :: term()}.
rmdir(Handle, FileId) ->
    ?MODULE:apply_helper_nif(Handle, rmdir, [FileId]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:symlink/3} function.
%% @end
%%--------------------------------------------------------------------
-spec symlink(helper_handle(), From :: file_id(), To :: file_id()) ->
    ok | {error, Reason :: term()}.
symlink(Handle, From, To) ->
    ?MODULE:apply_helper_nif(Handle, symlink, [From, To]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:rename/3} function.
%% @end
%%--------------------------------------------------------------------
-spec rename(helper_handle(), From :: file_id(), To :: file_id()) ->
    ok | {error, Reason :: term()}.
rename(Handle, From, To) ->
    ?MODULE:apply_helper_nif(Handle, rename, [From, To]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:link/3} function.
%% @end
%%--------------------------------------------------------------------
-spec link(helper_handle(), From :: file_id(), To :: file_id()) ->
    ok | {error, Reason :: term()}.
link(Handle, From, To) ->
    ?MODULE:apply_helper_nif(Handle, link, [From, To]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:chmod/3} function.
%% @end
%%--------------------------------------------------------------------
-spec chmod(helper_handle(), file_id(), Mode :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
chmod(Handle, FileId, Mode) ->
    ?MODULE:apply_helper_nif(Handle, chmod, [FileId, Mode]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:chown/4} function.
%% @end
%%--------------------------------------------------------------------
-spec chown(helper_handle(), file_id(), posix_user:uid(), posix_user:gid()) ->
    ok | {error, Reason :: term()}.
chown(Handle, FileId, UID, GID) ->
    ?MODULE:apply_helper_nif(Handle, chown, [FileId, UID, GID]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:truncate/3} function.
%% @end
%%--------------------------------------------------------------------
-spec truncate(helper_handle(), file_id(), Size :: non_neg_integer(),
    CurrentSize :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
truncate(Handle, FileId, Size, CurrentSize) ->
    ?MODULE:apply_helper_nif(Handle, truncate, [FileId, Size, CurrentSize]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:setxattr/6} function.
%% @end
%%--------------------------------------------------------------------
-spec setxattr(helper_handle(), file_id(), Name :: binary(),
    Value :: binary(), Create :: boolean(), Replace :: boolean()) ->
    ok | {error, Reason :: term()}.
setxattr(Handle, FileId, Name, Value, Create, Replace) ->
    ?MODULE:apply_helper_nif(Handle, setxattr, [FileId, Name, Value, Create, Replace]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:getxattr/3} function.
%% @end
%%--------------------------------------------------------------------
-spec getxattr(helper_handle(), file_id(), Name :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
getxattr(Handle, FileId, Name) ->
    ?MODULE:apply_helper_nif(Handle, getxattr, [FileId, Name]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:removexattr/3} function.
%% @end
%%--------------------------------------------------------------------
-spec removexattr(helper_handle(), file_id(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
removexattr(Handle, FileId, Name) ->
    ?MODULE:apply_helper_nif(Handle, removexattr, [FileId, Name]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:listxattr/2} function.
%% @end
%%--------------------------------------------------------------------
-spec listxattr(helper_handle(), file_id()) ->
    {ok, [binary()]} | {error, Reason :: term()}.
listxattr(Handle, FileId) ->
    ?MODULE:apply_helper_nif(Handle, listxattr, [FileId]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:open/3} function and constructs file handle record.
%% @end
%%--------------------------------------------------------------------
-spec open(helper_handle(), file_id(), open_flag()) ->
    {ok, file_handle()} | {error, Reason :: term()}.
open(#helper_handle{timeout = Timeout} = Handle, FileId, Flag) ->
    case ?MODULE:apply_helper_nif(Handle, open, [FileId, [open_flag_for_nif(Flag)]]) of
        {ok, FileHandle} ->
            {ok, #file_handle{handle = FileHandle, timeout = Timeout}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:read/3} function.
%% @end
%%--------------------------------------------------------------------
-spec read(file_handle(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | {error, Reason :: term()}.
read(Handle, Offset, Size) ->
    ?MODULE:apply_helper_nif(Handle, read, [Offset, Size]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:write/3} function.
%% @end
%%--------------------------------------------------------------------
-spec write(file_handle(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, Size :: non_neg_integer()} | {error, Reason :: term()}.
write(Handle, Offset, Data) ->
    ?MODULE:apply_helper_nif(Handle, write, [Offset, Data]).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:release/1} function.
%% @end
%%--------------------------------------------------------------------
-spec release(file_handle()) -> ok | {error, Reason :: term()}.
release(Handle) ->
    ?MODULE:apply_helper_nif(Handle, release, []).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:flush/1} function.
%% @end
%%--------------------------------------------------------------------
-spec flush(file_handle()) -> ok | {error, Reason :: term()}.
flush(Handle) ->
    ?MODULE:apply_helper_nif(Handle, flush, []).

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link helpers_nif:fsync/2} function.
%% @end
%%--------------------------------------------------------------------
-spec fsync(file_handle(), IsDataSync :: boolean()) ->
    ok | {error, Reason :: term()}.
fsync(Handle, IsDataSync) ->
    ?MODULE:apply_helper_nif(Handle, fsync, [boolean_for_nif(IsDataSync)]).

%%%===================================================================
%%% Exometer API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    Counters2 = lists:map(fun(Name) ->
        {?EXOMETER_TIME_NAME(Name), uniform, [{size, Size}]}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters ++ Counters2).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    Reports2 = lists:map(fun(Name) ->
        {?EXOMETER_TIME_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Reports ++ Reports2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls {@link apply_helper_nif/4} function with retrieved handle and timeout.
%% @end
%%--------------------------------------------------------------------
-spec apply_helper_nif(helper_handle() | file_handle(), Function :: atom(),
    Args :: [term()]) -> ok | {ok, term()} | {error, Reason :: term()}.
apply_helper_nif(#helper_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
    apply_helper_nif(Handle, Timeout, Function, Args);

apply_helper_nif(#file_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
    apply_helper_nif(Handle, Timeout, Function, Args).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls helpers NIF function and receives result or fails with timeout.
%% @end
%%--------------------------------------------------------------------
-spec apply_helper_nif(helpers_nif:helper_handle() | helpers_nif:file_handle(),
    timeout(), Function :: atom(), Args :: [term()]) ->
    ok | {ok, term()} | {error, Reason :: term()}.
apply_helper_nif(Handle, Timeout, Function, Args) ->
    ?update_counter(?EXOMETER_NAME(Function)),
    Now = os:timestamp(),
    {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
    Ans = receive_loop(ResponseRef, Timeout),
    Time = timer:now_diff(os:timestamp(), Now),
    ?update_counter(?EXOMETER_TIME_NAME(Function), Time),
    Ans.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for helper answer.
%% @end
%%--------------------------------------------------------------------
-spec receive_loop(helpers_nif:response_ref(), timeout()) ->
    ok | {ok, term()} | {error, Reason :: term()}.
receive_loop(ResponseRef, Timeout) ->
    receive
        {ResponseRef, heartbeat} ->
            receive_loop(ResponseRef, Timeout);
        {ResponseRef, Result} -> Result
    after
        Timeout -> {error, nif_timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a file type from helpers API to an argument for NIF function.
%% @end
%%--------------------------------------------------------------------
-spec file_type_for_nif(file_type_flag()) -> helpers_nif:file_type_flag().
file_type_for_nif(reg) -> 'S_IFREG';
file_type_for_nif(chr) -> 'S_IFCHR';
file_type_for_nif(blk) -> 'S_IFBLK';
file_type_for_nif(fifo) -> 'S_IFIFO';
file_type_for_nif(sock) -> 'S_IFSOCK'.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts an open flag from helpers API to an argument for NIF function.
%% @end
%%--------------------------------------------------------------------
-spec open_flag_for_nif(open_flag()) -> helpers_nif:open_flag().
open_flag_for_nif(write) -> 'O_WRONLY';
open_flag_for_nif(read) -> 'O_RDONLY';
open_flag_for_nif(rdwr) -> 'O_RDWR'.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a boolean to an argument for NIF function.
%% @end
%%--------------------------------------------------------------------
-spec boolean_for_nif(boolean()) -> 0 | 1.
boolean_for_nif(true) -> 1;
boolean_for_nif(false) -> 0.
