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
-module(helpers).
-author("Rafal Slota").

-include("modules/fslogic/helpers.hrl").

%% API
-export([new_handle/2]).
-export([getattr/2, access/3, mknod/4, mkdir/3, unlink/2, rmdir/2, symlink/3, rename/3, link/3, chmod/3, chown/4, truncate/3]).
-export([open/3, read/4, write/4, release/2, flush/2, fsync/3]).

%% Internal context record.
-record(helper_handle, {instance, ctx, timeout = timer:seconds(30)}).

-type file() :: binary().
-type error_code() :: atom().
-type handle() :: #helper_handle{}.

-export_type([file/0, error_code/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% new_handle/2
%%--------------------------------------------------------------------
%% @doc Creates new helper object along with helper context object. Valid within local Erlang-VM.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(HelperName :: helpers_nif:nif_string(), [Arg :: helpers_nif:nif_string()]) -> handle().
new_handle(HelperName, HelperArgs) ->
    {ok, Instance} = helpers_nif:new_helper_obj(HelperName, HelperArgs),
    {ok, CTX} = helpers_nif:new_helper_ctx(),
    #helper_handle{instance = Instance, ctx = CTX}.

%% getattr/2
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec getattr(handle(), File :: file()) -> {ok, #statbuf{}} | {error, term()}.
getattr(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, getattr, [File]).

%% access/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec access(handle(), File :: file(), Mask :: non_neg_integer()) -> ok | {error, term()}.
access(#helper_handle{} = HelperHandle, File, Mask) ->
    apply_helper_nif(HelperHandle, access, [File, Mask]).

%% mknod/4
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mknod(handle(), File :: file(), Mode :: non_neg_integer(), Type :: reg) -> ok | {error, term()}.
mknod(#helper_handle{} = HelperHandle, File, Mode, reg) ->
    apply_helper_nif(HelperHandle, mknod, [File, Mode bor helpers_nif:get_flag_value('S_IFREG'), 0]).

%% mkdir/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
mkdir(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mkdir, [File, Mode]).

%% unlink/2
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle(), File :: file()) -> ok | {error, term()}.
unlink(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, unlink, [File]).

%% rmdir/2
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle(), File :: file()) -> ok | {error, term()}.
rmdir(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, rmdir, [File]).

%% symlink/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec symlink(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
symlink(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, symlink, [From, To]).

%% rename/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rename(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
rename(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, rename, [From, To]).

%% link/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec link(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
link(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, link, [From, To]).

%% chmod/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
chmod(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, chmod, [File, Mode]).

%% chown/4
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chown(handle(), File :: file(), UID :: non_neg_integer() | -1, GID :: non_neg_integer() | -1) -> ok | {error, term()}.
chown(#helper_handle{} = HelperHandle, File, UID, GID) ->
    apply_helper_nif(HelperHandle, chown, [File, UID, GID]).

%% truncate/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), File :: file(), Size :: non_neg_integer()) -> ok | {error, term()}.
truncate(#helper_handle{} = HelperHandle, File, Size) ->
    apply_helper_nif(HelperHandle, truncate, [File, Size]).

%% open/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), File :: file(), OpenMode :: w | r | rw) -> {ok, FD :: non_neg_integer()} | {error, term()}.
open(#helper_handle{} = HelperHandle, File, w) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_WRONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, r) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, rw) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDWR']),
    apply_helper_nif(HelperHandle, open, [File]).

%% read/4
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec read(handle(), File :: file(),  Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | {error, term()}.
read(#helper_handle{} = HelperHandle, File, Offset, Size) ->
    apply_helper_nif(HelperHandle, read, [File, Offset, Size]).

%% write/4
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec write(handle(), File :: file(), Offset :: non_neg_integer(), Data :: binary()) ->
    {ok, Size :: non_neg_integer()} | {error, term()}.
write(#helper_handle{} = HelperHandle, File, Offset, Data) ->
    apply_helper_nif(HelperHandle, write, [File, Offset, Data]).

%% release/2
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec release(handle(), File :: file()) -> ok | {error, term()}.
release(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, release, [File]).

%% flush/2
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec flush(handle(), File :: file()) -> ok | {error, term()}.
flush(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, flush, [File]).

%% fsync/3
%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle(), File :: file(), IsDataSync :: boolean()) -> ok | {error, term()}.
fsync(#helper_handle{} = HelperHandle, File, true) ->
    apply_helper_nif(HelperHandle, fsync, [File, 1]);
fsync(#helper_handle{} = HelperHandle, File, false) ->
    apply_helper_nif(HelperHandle, fsync, [File, 0]).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% apply_helper_nif/3
%%--------------------------------------------------------------------
%% @doc Calls given helpers_nif method with given args while inserting HelperInstance :: resource_handle() and
%%      HelperCTX :: resource_handle() to this arguments list (from given #helper_handle{}.
%%      After call, receives and returns response.
%% @end
%%--------------------------------------------------------------------
-spec apply_helper_nif(handle(), Method :: atom(), [term()]) -> ok | {ok, term()} | {error, term()}.
apply_helper_nif(#helper_handle{instance = Instance, ctx = CTX, timeout = Timeout}, Method, Args) ->
    {ok, Guard} = apply(helpers_nif, Method, [Instance, CTX | Args]),
    receive
        {Guard, Result} ->
            Result
    after Timeout ->
        {error, nif_timeout}
    end.


-spec get_helper_ctx(handle()) -> helpers_nif:resource_handle().
get_helper_ctx(#helper_handle{ctx = CTX}) ->
    CTX.