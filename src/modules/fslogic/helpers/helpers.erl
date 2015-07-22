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
-export([new_handle/2, mkdir/3]).

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


mkdir(#helper_handle{} = HelperHandle, File, Mode) ->
    apply_helper_nif(HelperHandle, mkdir, [File, Mode]).

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
