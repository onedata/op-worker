%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Proxy for logical files manager operations
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_proxy).
-author("Tomasz Lichon").

-include_lib("common_test/include/ct.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("types.hrl").

%% API
-export([init/1, teardown/1, stat/3, truncate/4, create/4, unlink/3, open/4, close/2,
    read/4, write/4, mkdir/3, get_xattr/4, set_xattr/4, remove_xattr/4, list_xattr/3,
    write_and_check/4]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(Config :: list()) -> list().
init(Config) ->
    Host = self(),
    Servers = lists:map(
        fun(W) ->
            spawn_link(W,
                fun() ->
                    lfm_handles = ets:new(lfm_handles, [public, set, named_table]),
                    Host ! {self(), done},
                    receive
                        exit -> ok
                    end
                end)
        end, ?config(op_worker_nodes, Config)),

    lists:foreach(
        fun(Server) ->
            receive
                {Server, done} -> ok
            after timer:seconds(5) ->
                error("Cannot setup lfm_handles ETS")
            end
        end, Servers),

    [{servers, Servers} | Config].

-spec teardown(Config :: list()) -> ok.
teardown(Config) ->
    lists:foreach(
        fun(Pid) ->
            Pid ! exit
        end, ?config(servers, Config)).

-spec stat(node(), session:id(), file_key()) -> {ok, file_attributes()} | error_reply().
stat(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:stat(SessId, FileKey),
            Host ! {self(), Result}
        end).

-spec truncate(node(), session:id(), file_key(), non_neg_integer()) -> term().
truncate(Worker, SessId, FileKey, Size) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:truncate(SessId, FileKey, Size),
            Host ! {self(), Result}
        end).

-spec create(node(), session:id(), file_path(), file_meta:posix_permissions()) ->
    {ok, file_uuid()} | error_reply().
create(Worker, SessId, FilePath, Mode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:create(SessId, FilePath, Mode),
            Host ! {self(), Result}
        end).

-spec unlink(node(), session:id(), fslogic_worker:file()) -> ok | error_reply().
unlink(Worker, SessId, File) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:unlink(SessId, File),
            Host ! {self(), Result}
        end).

-spec open(node(), session:id(), FileKey :: file_id_or_path(), OpenType :: open_mode()) ->
    {ok, logical_file_manager:handle()} | error_reply().
open(Worker, SessId, FileKey, OpenMode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                case logical_file_manager:open(SessId, FileKey, OpenMode) of
                    {ok, Handle} ->
                        TestHandle = crypto:rand_bytes(10),
                        ets:insert(lfm_handles, {TestHandle, Handle}),
                        {ok, TestHandle};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec close(node(), logical_file_manager:handle()) -> ok| error_reply().
close(Worker, TestHandle) ->
    exec(Worker,
        fun(Host) ->
            ets:delete(lfm_handles, TestHandle),
            Host ! {self(), ok}
        end).

-spec read(node(), logical_file_manager:handle(), integer(), integer()) ->
    {ok, binary()} | error_reply().
read(Worker, TestHandle, Offset, Size) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case logical_file_manager:read(Handle, Offset, Size) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec write(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer()} | error_reply().
write(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case logical_file_manager:write(Handle, Offset, Bytes) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec write_and_check(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer(), StatAns} | error_reply() when
    StatAns :: {ok, file_attributes()} | error_reply().
write_and_check(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            #lfm_handle{file_uuid = UUID,
                fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle,
            Result =
                case logical_file_manager:write(Handle, Offset, Bytes) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        case logical_file_manager:fsync(NewHandle) of
                            ok ->
                                {ok, Res, logical_file_manager:stat(SessId, {uuid, UUID})};
                            Other2 ->
                                Other2
                        end;
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec mkdir(node(), session:id(), binary()) -> ok | error_reply().
mkdir(Worker, SessId, Path) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:mkdir(SessId, Path),
            Host ! {self(), Result}
        end).

-spec get_xattr(node(), session:id(), file_id_or_path(), xattr:name()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_xattr(SessId, FileKey, XattrKey),
            Host ! {self(), Result}
        end).

-spec set_xattr(node(), session:id(), file_id_or_path(), #xattr{}) -> ok | error_reply().
set_xattr(Worker, SessId, FileKey, Xattr) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_xattr(SessId, FileKey, Xattr),
            Host ! {self(), Result}
        end).

-spec remove_xattr(node(), session:id(), file_id_or_path(), xattr:name()) -> ok | error_reply().
remove_xattr(Worker, SessId, FileKey, XattrKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:remove_xattr(SessId, FileKey, XattrKey),
            Host ! {self(), Result}
        end).

-spec list_xattr(node(), session:id(), file_id_or_path()) -> {ok, [xattr:name()]} | error_reply().
list_xattr(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:list_xattr(SessId, FileKey),
            Host ! {self(), Result}
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

exec(Worker, Fun) ->
    Host = self(),
    Pid = spawn_link(Worker,
        fun() ->
            try
                Fun(Host)
            catch
                _:Reason ->
                    Host ! {self(), {error, {test_exec, Reason, erlang:get_stacktrace()}}}
            end
        end),
    receive
        {Pid, Result} -> Result
    after timer:seconds(5) ->
        {error, test_timeout}
    end.