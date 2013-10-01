%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module shoul be used as an proxy to veilhelpers_nif module.
%%       This module controls way of accessing veilhelpers_nif methods.
%% @end
%% ===================================================================

-module(veilhelpers).

-include("veil_modules/dao/dao_vfs.hrl").
-include_lib("registered_names.hrl").

-export([exec/2, exec/3]).
%% ===================================================================
%% API
%% ===================================================================


%% exec/3
%% ====================================================================
%% @doc Executes apply(veilhelper_nif, Method, Args) through slave node. <br/>
%%      Before executing, fields from struct SHInfo are preappend to Args list. <br/>
%%      You can also skip SHInfo argument in order to pass exact Args into target Method.    
%% @end
-spec exec(Method :: atom(), SHInfo :: #storage_helper_info{}, [Arg :: term()]) -> 
    {error, Reason :: term()} | Response when Response :: term().
%% ====================================================================
exec(Method, SHInfo = #storage_helper_info{}, Args) ->
    Args1 = [SHInfo#storage_helper_info.name | [SHInfo#storage_helper_info.init_args | Args]],
    exec(Method, Args1).
exec(Method, Args) when is_atom(Method), is_list(Args) ->
    lager:info("veilhelpers:exec with args: ~p ~p", [Method, Args]),
    case gsi_handler:call(veilhelpers_nif, Method, Args) of 
        {error, 'NIF_not_loaded'} ->
            ok = load_veilhelpers(),
            gsi_handler:call(veilhelpers_nif, Method, Args);
        Other -> Other
    end.


%% load_veilhelpers/0
%% ====================================================================
%% @doc Loads NIF library into slave node. Nodes are started and managed by {@link gsi_handler}
%% @end
-spec load_veilhelpers() -> ok | {error, Reason :: term()}.
%% ====================================================================
load_veilhelpers() ->
    {ok, Prefix} = application:get_env(?APP_Name, nif_prefix),
    case gsi_handler:call(veilhelpers_nif, start, [atom_to_list(Prefix)]) of 
        ok -> ok;
        {error,{reload, _}} -> ok;
        {error, Reason} -> 
            lager:error("Could not load veilhelpers NIF lib due to error: ~p", [Reason]),
            {error, Reason}
    end.