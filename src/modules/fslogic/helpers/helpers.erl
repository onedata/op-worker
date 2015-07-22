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

%% API
-export([new_handle/2]).
-export([getattr/2, access/3, mknod/3, mkdir/3, unlink/2, rmdir/2, symlink/3, rename/3, link/3, chmod/3, chown/4, truncate/3]).
-export([open/3, read/4, write/4, release/2, flush/2, fsync/3]).

-type file() :: binary().
-type error_code() :: atom().

-export_type([file/0, error_code/0]).

-record(helper_handle, {instance, ctx, timeout = timer:seconds(5)}).

%%%===================================================================
%%% API
%%%===================================================================

new_handle(HelperName, HelperArgs) ->
    {ok, Instance} = helpers_nif:new_helper_obj(HelperName, HelperArgs),
    {ok, CTX} = helpers_nif:new_helper_ctx(),
    #helper_handle{instance = Instance, ctx = CTX}.

getattr(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, getattr, [File]).

access(#helper_handle{} = HelperHandle, File, Mask) ->
    apply_helper_nif(HelperHandle, access, [File, Mask]).

mknod(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mknod, [File, Mode]).

mkdir(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mkdir, [File, Mode]).

unlink(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, unlink, [File]).

rmdir(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, rmdir, [File]).

symlink(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, symlink, [From, To]).

rename(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, rename, [From, To]).

link(#helper_handle{} = HelperHandle, From, To) ->
    apply_helper_nif(HelperHandle, link, [From, To]).

chmod(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, chmod, [File, Mode]).

chown(#helper_handle{} = HelperHandle, File, UID, GID) ->
    apply_helper_nif(HelperHandle, chown, [File, UID, GID]).

truncate(#helper_handle{} = HelperHandle, File, Size) ->
    apply_helper_nif(HelperHandle, truncate, [File, Size]).

open(#helper_handle{} = HelperHandle, File, w) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_WRONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, r) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDONLY']),
    apply_helper_nif(HelperHandle, open, [File]);
open(#helper_handle{} = HelperHandle, File, rw) ->
    helpers_nif:set_flags(get_helper_ctx(HelperHandle), ['O_RDWR']),
    apply_helper_nif(HelperHandle, open, [File]).


read(#helper_handle{} = HelperHandle, File, Offset, Size) ->
    apply_helper_nif(HelperHandle, read, [File, Offset, Size]).

write(#helper_handle{} = HelperHandle, File, Offset, Data) ->
    apply_helper_nif(HelperHandle, write, [File, Offset, Data]).

release(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, release, [File]).

flush(#helper_handle{} = HelperHandle, File) ->
    apply_helper_nif(HelperHandle, flush, [File]).

fsync(#helper_handle{} = HelperHandle, File, true) ->
    apply_helper_nif(HelperHandle, fsync, [File, 1]);
fsync(#helper_handle{} = HelperHandle, File, false) ->
    apply_helper_nif(HelperHandle, fsync, [File, 0]).

%%%===================================================================
%%% Internal functions
%%%===================================================================


apply_helper_nif(#helper_handle{instance = Instance, ctx = CTX, timeout = Timeout}, Method, Args) ->
    {ok, Guard} = apply(helpers_nif, Method, [Instance, CTX | Args]),
    receive
        {Guard, Result} ->
            Result
    after Timeout ->
        {error, nif_timeout}
    end.

get_helper_obj(#helper_handle{instance = Instance}) ->
    Instance.

get_helper_ctx(#helper_handle{ctx = CTX}) ->
    CTX.