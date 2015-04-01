%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc check_permissions annotation implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(check_permissions).
-annotation('function').
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5]).

-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {parent, item_definition()}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc annotation's before_advice implementation.
%%--------------------------------------------------------------------
-spec before_advice(#annotation{}, atom(), atom(), [term()]) -> term().
%% generic calls
before_advice(#annotation{data = []}, _M, _F, Args) ->
    Args;
before_advice(#annotation{data = [Obj | R]} = A, M, F, [#fslogic_ctx{} | _Inputs] = Args) ->
    before_advice(A#annotation{data = Obj}, M, F, Args),
    before_advice(A#annotation{data = R}, M, F, Args);

%% actual before_advice impl.
before_advice(#annotation{data = root}, _M, _F, [#fslogic_ctx{} | _Inputs] = Args) ->
    %% @todo: check if we are in root context
    Args;

before_advice(#annotation{data = {_AccessType, Item}}, _M, _F, [#fslogic_ctx{} | Inputs] = Args) ->
    _File = resolve_file(Item, Inputs),
    %% @todo: validate permissions of type AccessType for the File
    Args.


%%--------------------------------------------------------------------
%% @doc annotation's after_advice implementation.
%%--------------------------------------------------------------------
-spec after_advice(#annotation{}, atom(), atom(), [term()], term()) -> term().
after_advice(#annotation{}, _M, _F, _Inputs, Result) ->
    Result.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Extracts file() from argument list (Inputs) based on Item description.
%%--------------------------------------------------------------------
-spec resolve_file(item_definition(), [term()]) -> fslogic:file().
resolve_file(Item, Inputs) when Item > 0 ->
    lists:nth(Item - 1, Inputs);
resolve_file({path, Item}, Inputs) when Item > 0 ->
    {path, resolve_file(Item, Inputs)};
resolve_file({parent, Item}, Inputs) ->
    fslogic_utils:get_parent(resolve_file(Item, Inputs)).