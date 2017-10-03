%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model translates responses from Graph Sync channel into documents
%%% representing instances of entities.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_client_translator).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/api_errors.hrl").

%% API
-export([translate/2, apply_scope_mask/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates results of GET operations from Graph Sync channel.
%% CREATE operations do not require translation.
%% @end
%%--------------------------------------------------------------------
-spec translate(gs_protocol:gri(), Result :: gs_protocol:data()) ->
    datastore:document().
translate(#gri{type = od_user, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_user{
            name = maps:get(<<"name">>, Result),
            login = gs_protocol:null_to_undefined(maps:get(<<"login">>, Result)),
            email_list = maps:get(<<"emailList">>, Result),
            linked_accounts = maps:get(<<"linkedAccounts">>, Result),
            default_space = gs_protocol:null_to_undefined(maps:get(<<"defaultSpaceId">>, Result)),
            space_aliases = maps:get(<<"spaceAliases">>, Result),

            eff_groups = maps:get(<<"effectiveGroups">>, Result),
            eff_spaces = maps:get(<<"effectiveSpaces">>, Result),
            eff_handle_services = maps:get(<<"effectiveHandleServices">>, Result),
            eff_handles = maps:get(<<"effectiveHandles">>, Result)
        }
    };

translate(#gri{type = od_user, id = Id, aspect = instance, scope = protected}, Result) ->
    #document{
        key = Id,
        value = #od_user{
            name = maps:get(<<"name">>, Result),
            login = gs_protocol:null_to_undefined(maps:get(<<"login">>, Result)),
            email_list = maps:get(<<"emailList">>, Result),
            linked_accounts = maps:get(<<"linkedAccounts">>, Result)
        }
    };

translate(#gri{type = od_user, id = Id, aspect = instance, scope = shared}, Result) ->
    #document{
        key = Id,
        value = #od_user{
            name = maps:get(<<"name">>, Result),
            login = gs_protocol:null_to_undefined(maps:get(<<"login">>, Result))
        }
    };

translate(#gri{type = od_group, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_group{
            name = maps:get(<<"name">>, Result),
            type = binary_to_atom(maps:get(<<"type">>, Result), utf8),

            direct_children = privileges_to_atoms(maps:get(<<"children">>, Result)),
            eff_children = privileges_to_atoms(maps:get(<<"effectiveChildren">>, Result)),
            direct_parents = maps:get(<<"parents">>, Result),

            direct_users = privileges_to_atoms(maps:get(<<"users">>, Result)),
            eff_users = privileges_to_atoms(maps:get(<<"effectiveUsers">>, Result)),

            eff_spaces = maps:get(<<"spaces">>, Result)
        }
    };

% shared and protected scopes carry the same data
translate(GRI = #gri{type = od_group, aspect = instance, scope = shared}, Result) ->
    translate(GRI#gri{scope = protected}, Result);
translate(#gri{type = od_group, id = Id, scope = protected}, Result) ->
    #document{
        key = Id,
        value = #od_group{
            name = maps:get(<<"name">>, Result),
            type = binary_to_atom(maps:get(<<"type">>, Result), utf8)
        }
    };

translate(#gri{type = od_space, id = Id, aspect = instance, scope = protected}, Result) ->
    #document{
        key = Id,
        value = #od_space{
            name = maps:get(<<"name">>, Result)
        }
    };

translate(#gri{type = od_space, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_space{
            name = maps:get(<<"name">>, Result),

            direct_users = privileges_to_atoms(maps:get(<<"users">>, Result)),
            eff_users = privileges_to_atoms(maps:get(<<"effectiveUsers">>, Result)),

            direct_groups = privileges_to_atoms(maps:get(<<"groups">>, Result)),
            eff_groups = privileges_to_atoms(maps:get(<<"effectiveGroups">>, Result)),

            providers = maps:get(<<"providers">>, Result),
            shares = maps:get(<<"shares">>, Result)
        }
    };

% private and protected scopes carry the same data
translate(#gri{type = od_share, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_share{
            name = maps:get(<<"name">>, Result),
            public_url = maps:get(<<"publicUrl">>, Result),
            space = maps:get(<<"spaceId">>, Result),
            handle = gs_protocol:null_to_undefined(maps:get(<<"handleId">>, Result)),
            root_file = maps:get(<<"rootFileId">>, Result)
        }
    };
translate(#gri{type = od_share, id = Id, aspect = instance, scope = public}, Result) ->
    #document{
        key = Id,
        value = #od_share{
            name = maps:get(<<"name">>, Result),
            public_url = maps:get(<<"publicUrl">>, Result),
            handle = gs_protocol:null_to_undefined(maps:get(<<"handleId">>, Result)),
            root_file = maps:get(<<"rootFileId">>, Result)
        }
    };

translate(#gri{type = od_provider, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_provider{
            name = maps:get(<<"name">>, Result),
            urls = maps:get(<<"urls">>, Result),
            spaces = maps:get(<<"spaces">>, Result),
            eff_users = maps:get(<<"effectiveUsers">>, Result),
            eff_groups = maps:get(<<"effectiveGroups">>, Result)
        }
    };

translate(#gri{type = od_provider, id = Id, aspect = instance, scope = protected}, Result) ->
    #document{
        key = Id,
        value = #od_provider{
            name = maps:get(<<"name">>, Result),
            urls = maps:get(<<"urls">>, Result)
        }
    };

translate(#gri{type = od_handle_service, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_handle_service{
            name = maps:get(<<"name">>, Result),
            eff_users = privileges_to_atoms(maps:get(<<"effectiveUsers">>, Result)),
            eff_groups = privileges_to_atoms(maps:get(<<"effectiveGroups">>, Result))
        }
    };

translate(#gri{type = od_handle, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_handle{
            public_handle = maps:get(<<"publicHandle">>, Result),
            resource_type = maps:get(<<"resourceType">>, Result),
            resource_id = maps:get(<<"resourceId">>, Result),
            metadata = maps:get(<<"metadata">>, Result),
            timestamp = timestamp_utils:datestamp_to_datetime(maps:get(<<"timestamp">>, Result)),
            handle_service = maps:get(<<"handleServiceId">>, Result),

            eff_users = privileges_to_atoms(maps:get(<<"effectiveUsers">>, Result)),
            eff_groups = privileges_to_atoms(maps:get(<<"effectiveGroups">>, Result))
        }
    };

translate(#gri{type = od_handle, id = Id, aspect = instance, scope = public}, Result) ->
    #document{
        key = Id,
        value = #od_handle{
            public_handle = maps:get(<<"publicHandle">>, Result),
            metadata = maps:get(<<"metadata">>, Result),
            timestamp = timestamp_utils:datestamp_to_datetime(maps:get(<<"timestamp">>, Result))
        }
    };

translate(GRI, Result) ->
    ?error("Cannot translate graph sync response body for:~nGRI: ~p~nResult: ~p~n", [
        GRI, Result
    ]),
    throw(?ERROR_INTERNAL_SERVER_ERROR).


%%--------------------------------------------------------------------
%% @doc
%% Masks (nullifies) the fields in given record that should not be available
%% within given scope.
%% @end
%%--------------------------------------------------------------------
-spec apply_scope_mask(datastore:document(), gs_protocol:scope()) ->
    datastore:document().
apply_scope_mask(Doc = #document{value = User = #od_user{}}, protected) ->
    Doc#document{
        value = User#od_user{
            default_space = undefined,
            space_aliases = #{},

            eff_groups = [],
            eff_spaces = [],
            eff_handle_services = [],
            eff_handles = []
        }
    };
apply_scope_mask(Doc = #document{value = User = #od_user{}}, shared) ->
    Doc#document{
        value = User#od_user{
            email_list = [],
            linked_accounts = [],
            default_space = undefined,
            space_aliases = #{},

            eff_groups = [],
            eff_spaces = [],
            eff_handle_services = [],
            eff_handles = []
        }
    };

apply_scope_mask(Doc = #document{value = #od_group{}}, protected) ->
    apply_scope_mask(Doc, shared);
apply_scope_mask(Doc = #document{value = Group = #od_group{}}, shared) ->
    Doc#document{
        value = Group#od_group{
            direct_children = #{},
            eff_children = #{},
            direct_parents = [],

            direct_users = #{},
            eff_users = #{},

            eff_spaces = []
        }
    };

apply_scope_mask(Doc = #document{value = Space = #od_space{}}, protected) ->
    Doc#document{
        value = Space#od_space{
            direct_users = #{},
            eff_users = #{},

            direct_groups = #{},
            eff_groups = #{},

            providers = #{},
            shares = []
        }
    };

apply_scope_mask(Doc = #document{value = Share = #od_share{}}, public) ->
    Doc#document{
        value = Share#od_share{
            space = undefined
        }
    };

apply_scope_mask(Doc = #document{value = Provider = #od_provider{}}, protected) ->
    Doc#document{
        value = Provider#od_provider{
            spaces = #{},
            eff_users = [],
            eff_groups = []
        }
    };

apply_scope_mask(Doc = #document{value = Handle = #od_handle{}}, public) ->
    Doc#document{
        value = Handle#od_handle{
            resource_type = undefined,
            resource_id = undefined,
            handle_service = undefined,

            eff_users = #{},
            eff_groups = #{}
        }
    }.


-spec privileges_to_atoms(maps:map(binary(), binary())) -> maps:map(binary(), atom()).
privileges_to_atoms(Map) ->
    maps:map(
        fun(_Key, Privileges) ->
            [binary_to_atom(P, utf8) || P <- Privileges]
        end, Map).
