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

-include("modules/fslogic/helpers.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").



%% API
-export([new_handle/1, new_handle/2]).
-export([getattr/2, access/3, mknod/4, mkdir/3, unlink/2, rmdir/2, symlink/3, rename/3, link/3, chmod/3]).
-export([chown/4, truncate/3, open/3, read/4, write/4, release/2, flush/2, fsync/3]).

%% Internal context record.
-record(helper_handle, {instance, ctx, timeout = timer:seconds(30)}).

-type file() :: binary().
-type open_mode() :: write | read | rdwr.
-type error_code() :: atom().
-type handle() :: #helper_handle{}.
-type name() :: binary().
-type args() :: #{binary() => binary()}.

-export_type([file/0, open_mode/0, error_code/0, handle/0, name/0, args/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% new_handle/1
%%--------------------------------------------------------------------
%% @doc Creates new helper object along with helper context object. Valid within local Erlang-VM.
%%      @todo: maybe cache new_helper_obj result
%% @end
%%--------------------------------------------------------------------
-spec new_handle(#helper_init{}) -> handle().
new_handle(#helper_init{name = Name, args = Args}) ->
    new_handle(Name, Args).


%%--------------------------------------------------------------------
%% @doc Creates new helper object along with helper context object. Valid within local Erlang-VM.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(HelperName :: helpers_nif:nif_string(), [Arg :: helpers_nif:nif_string()] | args()) -> handle().
new_handle(HelperName, HelperArgs) when is_map(HelperArgs) ->
    new_handle(HelperName, maps:values(HelperArgs));
new_handle(HelperName, HelperArgs) ->
    ?debug("helpers:new_handle ~p ~p", [HelperName, HelperArgs]),
    {ok, Instance} = helpers_nif:new_helper_obj(HelperName, HelperArgs),
    {ok, CTX} = helpers_nif:new_helper_ctx(),
    #helper_handle{instance = Instance, ctx = CTX}.

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec getattr(handle(), File :: file()) -> {ok, #statbuf{}} | {error, term()}.
getattr(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, getattr, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec access(handle(), File :: file(), Mask :: non_neg_integer()) -> ok | {error, term()}.
access(#helper_handle{} = HelperHandle, File, Mask) ->
    apply_helper_nif(HelperHandle, access, [File, Mask]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mknod(handle(), File :: file(), Mode :: non_neg_integer(), Type :: reg) -> ok | {error, term()}.
mknod(#helper_handle{} = HelperHandle, File, Mode, reg) ->
    apply_helper_nif(HelperHandle, mknod, [File, Mode bor helpers_nif:get_flag_value('S_IFREG'), 0]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
mkdir(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mkdir, [File, Mode]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle(), File :: file()) -> ok | {error, term()}.
unlink(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, unlink, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle(), File :: file()) -> ok | {error, term()}.
rmdir(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, rmdir, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec symlink(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
symlink(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, symlink, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec rename(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
rename(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, rename, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec link(handle(), From :: file(), To :: file()) -> ok | {error, term()}.
link(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, link, [From, To]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), File :: file(), Mode :: non_neg_integer()) -> ok | {error, term()}.
chmod(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, chmod, [File, Mode]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec chown(handle(), File :: file(), UID :: non_neg_integer() | -1, GID :: non_neg_integer() | -1) -> ok | {error, term()}.
chown(#helper_handle{} = HelperHandle, File, UID, GID) ->
    apply_helper_nif(HelperHandle, chown, [File, UID, GID]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), File :: file(), Size :: non_neg_integer()) -> ok | {error, term()}.
truncate(#helper_handle{} = HelperHandle, File, Size) ->
    apply_helper_nif(HelperHandle, truncate, [File, Size]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), File :: file(), OpenMode :: open_mode()) -> {ok, FD :: non_neg_integer()} | {error, term()}.
open(#helper_handle{} = HelperHandle, File, write) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_WRONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, read) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, rdwr) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDWR']),
    apply_helper_nif(HelperHandle, open, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec read(handle(), File :: file(),  Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
                  {ok, Data :: binary()} | {error, term()}.
read(#helper_handle{} = HelperHandle, File, Offset, Size) ->
    apply_helper_nif(HelperHandle, read, [File, Offset, Size]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec write(handle(), File :: file(), Offset :: non_neg_integer(), Data :: binary()) ->
                   {ok, Size :: non_neg_integer()} | {error, term()}.
write(#helper_handle{} = HelperHandle, File, Offset, Data) ->
    apply_helper_nif(HelperHandle, write, [File, Offset, Data]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec release(handle(), File :: file()) -> ok | {error, term()}.
release(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, release, [File]).

%%--------------------------------------------------------------------
%% @doc Calls the corresponding helper_nif method and receives result.
%%      First argument shall be #helper_handle{} from new_handle/2.
%% @end
%%--------------------------------------------------------------------
-spec flush(handle(), File :: file()) -> ok | {error, term()}.
flush(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, flush, [File]).

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


%%--------------------------------------------------------------------
%% @private
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