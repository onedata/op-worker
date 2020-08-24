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
-include("modules/storage/storage.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

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
-spec translate(gri:gri(), Result :: gs_protocol:data()) ->
    datastore:doc().
translate(#gri{type = od_provider, aspect = current_time}, #{<<"timeMillis">> := TimeMillis}) ->
    TimeMillis;


translate(#gri{type = od_user, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_user{
            full_name = maps:get(<<"fullName">>, Result, maps:get(<<"name">>, Result, undefined)),
            username = utils:null_to_undefined(maps:get(<<"username">>, Result, maps:get(<<"alias">>, Result, null))),
            emails = maps:get(<<"emails">>, Result, maps:get(<<"emailList">>, Result, [])),
            linked_accounts = maps:get(<<"linkedAccounts">>, Result),

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
            full_name = maps:get(<<"fullName">>, Result, maps:get(<<"name">>, Result, undefined)),
            username = utils:null_to_undefined(maps:get(<<"username">>, Result, maps:get(<<"alias">>, Result, null))),
            emails = maps:get(<<"emails">>, Result, maps:get(<<"emailList">>, Result, [])),
            linked_accounts = maps:get(<<"linkedAccounts">>, Result)
        }
    };

translate(#gri{type = od_user, id = Id, aspect = instance, scope = shared}, Result) ->
    #document{
        key = Id,
        value = #od_user{
            full_name = maps:get(<<"fullName">>, Result, maps:get(<<"name">>, Result, undefined)),
            username = utils:null_to_undefined(maps:get(<<"username">>, Result, maps:get(<<"alias">>, Result, null)))
        }
    };

translate(#gri{type = od_group, id = Id, scope = shared}, Result) ->
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
            name = maps:get(<<"name">>, Result),
            providers = maps:get(<<"providers">>, Result)
        }
    };


translate(#gri{type = od_space, id = SpaceId, aspect = instance, scope = private}, Result) ->
    Storages = maps:get(<<"storages">>, Result),

    % the space might be fetched with user's session, in such case it is not
    % guaranteed that the provider supports the space and can fetch storages
    StoragesByProvider = case provider_logic:supports_space(SpaceId) of
        false ->
            #{};
        true ->
            lists:foldl(fun(StorageId, Acc) ->
                {ok, ProviderId} = storage_logic:get_provider(StorageId, SpaceId),
                AccessType = case storage:is_storage_readonly(StorageId, SpaceId) of
                    true -> ?READONLY_STORAGE;
                    false -> ?READWRITE_STORAGE
                end,
                maps:update_with(ProviderId, fun(ProviderStorages) ->
                    ProviderStorages#{StorageId => AccessType}
                end, #{StorageId => AccessType}, Acc)
            end, #{}, maps:keys(Storages))
    end,

    #document{
        key = SpaceId,
        value = #od_space{
            name = maps:get(<<"name">>, Result),

            owners = maps:get(<<"owners">>, Result, []),

            direct_users = privileges_to_atoms(maps:get(<<"users">>, Result)),
            eff_users = privileges_to_atoms(maps:get(<<"effectiveUsers">>, Result)),

            direct_groups = privileges_to_atoms(maps:get(<<"groups">>, Result)),
            eff_groups = privileges_to_atoms(maps:get(<<"effectiveGroups">>, Result)),

            storages = Storages,
            storages_by_provider = StoragesByProvider,

            providers = maps:get(<<"providers">>, Result),
            shares = maps:get(<<"shares">>, Result),
            harvesters = maps:get(<<"harvesters">>, Result)
        }
    };

% private and protected scopes carry the same data
translate(#gri{type = od_share, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_share{
            name = maps:get(<<"name">>, Result),
            description = maps:get(<<"description">>, Result),
            public_url = maps:get(<<"publicUrl">>, Result),
            file_type = binary_to_existing_atom(maps:get(<<"fileType">>, Result), utf8),
            space = maps:get(<<"spaceId">>, Result),
            handle = utils:null_to_undefined(maps:get(<<"handleId">>, Result)),
            root_file = maps:get(<<"rootFileId">>, Result)
        }
    };
translate(#gri{type = od_share, id = Id, aspect = instance, scope = public}, Result) ->
    #document{
        key = Id,
        value = #od_share{
            name = maps:get(<<"name">>, Result),
            description = maps:get(<<"description">>, Result),
            public_url = maps:get(<<"publicUrl">>, Result),
            file_type = binary_to_existing_atom(maps:get(<<"fileType">>, Result), utf8),
            handle = utils:null_to_undefined(maps:get(<<"handleId">>, Result)),
            root_file = maps:get(<<"rootFileId">>, Result)
        }
    };

translate(#gri{type = od_provider, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_provider{
            name = maps:get(<<"name">>, Result),
            admin_email = maps:get(<<"adminEmail">>, Result),
            subdomain_delegation = maps:get(<<"subdomainDelegation">>, Result),
            domain = maps:get(<<"domain">>, Result),
            subdomain = maps:get(<<"subdomain">>, Result),
            longitude = maps:get(<<"longitude">>, Result),
            latitude = maps:get(<<"latitude">>, Result),
            online = maps:get(<<"online">>, Result),
            eff_spaces = maps:get(<<"effectiveSpaces">>, Result),
            storages = maps:get(<<"storages">>, Result),
            eff_users = maps:get(<<"effectiveUsers">>, Result),
            eff_groups = maps:get(<<"effectiveGroups">>, Result)
        }
    };

translate(#gri{type = od_provider, id = Id, aspect = instance, scope = protected}, Result) ->
    #document{
        key = Id,
        value = #od_provider{
            name = maps:get(<<"name">>, Result),
            domain = maps:get(<<"domain">>, Result),
            longitude = maps:get(<<"longitude">>, Result),
            latitude = maps:get(<<"latitude">>, Result),
            online = maps:get(<<"online">>, Result)
        }
    };

translate(#gri{type = od_provider, id = _Id, aspect = domain_config}, Result) ->
    case maps:find(<<"ipList">>, Result) of
        {ok, IPBinaries} ->
            IPTuples = lists:map(fun(IPBin) ->
                {ok, IP} = inet:parse_ipv4strict_address(binary_to_list(IPBin)),
                IP
            end, IPBinaries),
            Result#{<<"ipList">> := IPTuples};
        error ->
            Result
    end;

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
            timestamp = time_utils:datestamp_to_datetime(maps:get(<<"timestamp">>, Result)),
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
            timestamp = time_utils:datestamp_to_datetime(maps:get(<<"timestamp">>, Result))
        }
    };

translate(#gri{type = od_harvester, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_harvester{
            indices = maps:get(<<"indices">>, Result),
            spaces = maps:get(<<"spaces">>, Result)
        }
    };

translate(#gri{type = od_storage, id = Id, aspect = instance, scope = private}, Result) ->
    #document{
        key = Id,
        value = #od_storage{
            name = maps:get(<<"name">>, Result),
            provider = maps:get(<<"provider">>, Result),
            spaces = maps:get(<<"spaces">>, Result),
            qos_parameters = maps:get(<<"qosParameters">>, Result),
            imported = maps:get(<<"imported">>, Result),
            readonly = maps:get(<<"readonly">>, Result)
        }
    };

translate(#gri{type = od_storage, id = Id, aspect = instance, scope = shared}, Result) ->
    #document{
        key = Id,
        value = #od_storage{
            provider = maps:get(<<"provider">>, Result),
            qos_parameters = maps:get(<<"qosParameters">>, Result),
            readonly = maps:get(<<"readonly">>, Result)
        }
    };

translate(#gri{type = od_token, id = Id, aspect = instance, scope = shared}, Result) ->
    #document{
        key = Id,
        value = #od_token{revoked = maps:get(<<"revoked">>, Result)}
    };

translate(#gri{type = temporary_token_secret, id = Id, aspect = user, scope = shared}, Result) ->
    #document{
        key = Id,
        value = #temporary_token_secret{generation = maps:get(<<"generation">>, Result)}
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
-spec apply_scope_mask(datastore:doc(), gs_protocol:scope()) ->
    datastore:doc().
apply_scope_mask(Doc = #document{value = User = #od_user{}}, protected) ->
    Doc#document{
        value = User#od_user{
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
            emails = [],
            linked_accounts = [],
            space_aliases = #{},

            eff_groups = [],
            eff_spaces = [],
            eff_handle_services = [],
            eff_handles = []
        }
    };

apply_scope_mask(Doc = #document{value = Space = #od_space{}}, protected) ->
    Doc#document{
        value = Space#od_space{
            owners = [],

            direct_users = #{},
            eff_users = #{},

            direct_groups = #{},
            eff_groups = #{},

            shares = [],
            harvesters = []
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
            admin_email = undefined,
            subdomain_delegation = undefined,
            subdomain = undefined,
            eff_spaces = #{},
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
    };

apply_scope_mask(Doc = #document{value = Storage = #od_storage{}}, shared) ->
    Doc#document{
        value = Storage#od_storage{
            name = undefined,
            spaces = [],
            imported = undefined
        }
    }.


-spec privileges_to_atoms(#{binary() => binary()}) -> #{binary() => atom()}.
privileges_to_atoms(Map) ->
    maps:map(
        fun(_Key, Privileges) ->
            [binary_to_atom(P, utf8) || P <- Privileges]
        end, Map).
