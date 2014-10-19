%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains code for file_manager permissions editor component.
%% It consists of several functions that render n2o elements and implement logic.
%% @end
%% ===================================================================
-module(pfm_perms).

-include("oneprovider_modules/control_panel/common.hrl").

%% API
-export([init/0, api_event/3]).


%% init/0
%% ====================================================================
%% @doc Initializes perms editor component.
%% @end
-spec init() -> ok.
%% ====================================================================
init() ->
    gui_jq:wire(#api{name = "change_perms_type_event", tag = "change_perms_type_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_perms_event", tag = "submit_perms_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "add_acl_event", tag = "add_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "delete_acl_event", tag = "delete_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "edit_acl_event", tag = "edit_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "move_acl_event", tag = "move_acl_event", delegate = ?MODULE}, false),
    gui_jq:wire(#api{name = "submit_acl_event", tag = "submit_acl_event", delegate = ?MODULE}, false).


api_event("submit_perms_event", Args, _Ctx) ->
    [Perms, Recursive] = mochijson2:decode(Args),
    eval_in_comet(submit_perms, [Perms, Recursive]);

api_event("change_perms_type_event", Args, _Ctx) ->
    EnableACL = mochijson2:decode(Args),
    eval_in_comet(change_perms_type, [EnableACL]);

api_event("add_acl_event", _Args, _) ->
    eval_in_comet(add_acl, []);

api_event("delete_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(delete_acl, [Index]);

api_event("edit_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(edit_acl, [Index]);

api_event("move_acl_event", Args, _) ->
    [IndexRaw, MoveUp] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(move_acl, [Index, MoveUp]);

api_event("submit_acl_event", Args, _) ->
    [IndexRaw, Identifier, Type, Read, Write, Execute] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    eval_in_comet(submit_acl, [Index, Identifier, Type, Read, Write, Execute]).


eval_in_comet(Fun, Args) ->
    opn_gui_utils:apply_or_redirect(erlang, send, [get(comet_pid), {action, ?MODULE, Fun, Args}]).