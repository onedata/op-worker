%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (get) corresponding to file aspects such as:
%%% - attributes,
%%% - extended attributes,
%%% - json metadata,
%%% - rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_get_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/privileges.hrl").


-export([assert_operation_supported/2]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([get/2]).


-define(DEFAULT_LIST_OFFSET, 0).
-define(DEFAULT_LIST_ENTRIES, 1000).
-define(MAX_LIST_ENTRIES, 10000).
-define(MAX_LIST_OFFSET, 500).

-define(DEFAULT_LIST_ATTRS, [guid, name]).
-define(DEFAULT_RECURSIVE_FILE_LIST_ATTRS, [guid, path]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).


%%%===================================================================
%%% API
%%%===================================================================

-spec assert_operation_supported(gri:aspect(), middleware:scope()) ->
    ok | no_return().
assert_operation_supported(instance, private)                        -> ok;    % REST/gs
assert_operation_supported(instance, public)                         -> ok;    % REST/gs
assert_operation_supported(children, private)                        -> ok;    % REST/gs
assert_operation_supported(children, public)                         -> ok;    % REST/gs
assert_operation_supported(files, private)                           -> ok;    % REST only
assert_operation_supported(xattrs, private)                          -> ok;    % REST/gs
assert_operation_supported(xattrs, public)                           -> ok;    % REST/gs
assert_operation_supported(json_metadata, private)                   -> ok;    % REST/gs
assert_operation_supported(json_metadata, public)                    -> ok;    % REST/gs
assert_operation_supported(rdf_metadata, private)                    -> ok;    % REST/gs
assert_operation_supported(rdf_metadata, public)                     -> ok;    % REST/gs
assert_operation_supported(distribution, private)                    -> ok;    % REST/gs
assert_operation_supported(storage_locations, private)               -> ok;
assert_operation_supported(acl, private)                             -> ok;
assert_operation_supported(shares, private)                          -> ok;    % gs only
assert_operation_supported(transfers, private)                       -> ok;
assert_operation_supported(qos_summary, private)                     -> ok;    % REST/gs
assert_operation_supported(dataset_summary, private)                 -> ok;
assert_operation_supported(download_url, private)                    -> ok;    % gs only
assert_operation_supported(download_url, public)                     -> ok;    % gs only
assert_operation_supported(hardlinks, private)                       -> ok;
assert_operation_supported({hardlinks, _}, private)                  -> ok;
assert_operation_supported(symlink_value, public)                    -> ok;
assert_operation_supported(symlink_value, private)                   -> ok;
assert_operation_supported(symlink_target, public)                   -> ok;
assert_operation_supported(symlink_target, private)                  -> ok;
assert_operation_supported(archive_recall_details, private)          -> ok;
assert_operation_supported(archive_recall_progress, private)         -> ok;
assert_operation_supported(archive_recall_log, private)              -> ok;
assert_operation_supported(api_samples, public)                      -> ok;
assert_operation_supported(api_samples, private)                     -> ok;
assert_operation_supported(dir_size_stats_collection_schema, public) -> ok;
assert_operation_supported({dir_size_stats_collection, _}, private)  -> ok;
assert_operation_supported(_, _)                                     -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================

-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{gri = #gri{aspect = instance, scope = Sc}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"attributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, current, <<"attributes">>),
        % @TODO VFS-11377 deprecated, left for backwards compatibility
        <<"attribute">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, deprecated, <<"attribute">>)
    }
};

data_spec(#op_req{gri = #gri{aspect = children, scope = Sc}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_ENTRIES}},
        <<"token">> => build_listing_start_point_param_spec(<<"token">>),
        <<"index">> => build_listing_start_point_param_spec(<<"index">>),
        <<"offset">> => {integer, {between, -?MAX_LIST_OFFSET, ?MAX_LIST_OFFSET}},
        <<"inclusive">> => {boolean, any},
        <<"tune_for_large_continuous_listing">> => {boolean, any},
        <<"tuneForLargeContinuousListing">> => {boolean, any},
        <<"attributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, current, <<"attributes">>),
        % @TODO VFS-11377 deprecated, left for backwards compatibility
        <<"attribute">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, deprecated, <<"attribute">>)
    }
};

data_spec(#op_req{gri = #gri{aspect = files, scope = Sc}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_ENTRIES}},
        <<"token">> => {binary, any},
        <<"prefix">> => {binary, any},
        <<"start_after">> => {binary, any},
        <<"include_directories">> => {boolean, any},
        <<"attributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, current, <<"attributes">>),
        % @TODO VFS-11377 deprecated, left for backwards compatibility
        <<"attribute">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, deprecated_recursive, <<"attribute">>)
    }
};

data_spec(#op_req{gri = #gri{aspect = xattrs}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"attribute">> => {binary, non_empty},
        <<"inherited">> => {boolean, any},
        <<"show_internal">> => {boolean, any},
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec(#op_req{gri = #gri{aspect = json_metadata}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"filter_type">> => {binary, [<<"keypath">>]},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any},
        <<"resolve_symlink">> => {boolean, any}
    }
};

data_spec(#op_req{gri = #gri{aspect = rdf_metadata}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"resolve_symlink">> => {boolean, any}}
};

data_spec(#op_req{gri = #gri{aspect = symlink_target, scope = Sc}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{
        <<"attributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
            Sc, current, <<"attributes">>)
    }
};

data_spec(#op_req{gri = #gri{aspect = As}}) when
    As =:= distribution;
    As =:= storage_locations;
    As =:= acl;
    As =:= shares;
    As =:= symlink_value;
    As =:= archive_recall_details;
    As =:= archive_recall_progress;
    As =:= api_samples
->
    #{required => #{id => {binary, guid}}};

data_spec(#op_req{gri = #gri{aspect = hardlinks}}) ->
    #{
        required => #{id => {binary, guid}},
        optional => #{<<"limit">> => {integer, {not_lower_than, 1}}}
    };

data_spec(#op_req{gri = #gri{aspect = {hardlinks, _}}}) -> #{
    required => #{
        id => {binary, guid},
        {aspect, <<"guid">>} => {binary, guid}
    }
};

data_spec(#op_req{gri = #gri{aspect = transfers}}) -> #{
    required => #{id => {binary, guid}},
    optional => #{<<"include_ended_ids">> => {boolean, any}}
};

data_spec(#op_req{gri = #gri{aspect = As}}) when
    As =:= qos_summary;
    As =:= dataset_summary
-> #{
    required => #{id => {binary, guid}}
};

data_spec(#op_req{gri = #gri{aspect = download_url}}) -> #{
    required => #{<<"file_ids">> => {list_of_binaries, guid}},
    optional => #{<<"follow_symlinks">> => {boolean, any}}
};

data_spec(#op_req{gri = #gri{aspect = archive_recall_log}}) ->
    audit_log_browse_opts:json_data_spec();

data_spec(#op_req{gri = #gri{aspect = dir_size_stats_collection_schema}}) -> #{};

data_spec(#op_req{gri = #gri{aspect = {dir_size_stats_collection, _}}}) -> #{
    required => #{
        id => {binary, guid}
    },
    % for this aspect data is sanitized in `get` function, but all possible parameters
    % still have to be specified so they are not removed during sanitization
    optional => #{
        <<"mode">> => {any, any},
        <<"layout">> => {any, any},
        <<"startTimestamp">> => {any, any},
        <<"stopTimestamp">> => {any, any},
        <<"windowLimit">> => {any, any},
        <<"extendedInfo">> => {any, any}
    }
}.


-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{gri = #gri{id = undefined, aspect = dir_size_stats_collection_schema, scope = public}}, _) ->
    true;

authorize(#op_req{gri = #gri{id = FileGuid, aspect = As, scope = public}}, _) when
    As =:= instance;
    As =:= children;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= api_samples
->
    file_id:is_share_guid(FileGuid);

authorize(#op_req{auth = Auth, gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= children;
    As =:= files;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= distribution;
    As =:= storage_locations;
    As =:= acl;
    As =:= shares;
    As =:= dataset_summary;
    As =:= hardlinks;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= archive_recall_details;
    As =:= archive_recall_progress;
    As =:= archive_recall_log;
    element(1, As) =:= dir_size_stats_collection;
    As =:= api_samples
->
    middleware_utils:has_access_to_file_space(Auth, Guid);

authorize(#op_req{auth = Auth, gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    middleware_utils:has_access_to_file_space(Auth, FirstGuid) andalso
        middleware_utils:has_access_to_file_space(Auth, SecondGuid);

authorize(#op_req{auth = ?USER(UserId), gri = #gri{id = Guid, aspect = transfers}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{auth = ?USER(UserId), gri = #gri{id = Guid, aspect = qos_summary}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_QOS);

authorize(#op_req{auth = Auth, gri = #gri{aspect = download_url, scope = Scope}, data = Data}, _) ->
    Predicate = case Scope of
        private ->
            fun(Guid) ->
                not file_id:is_share_guid(Guid)
                    andalso middleware_utils:has_access_to_file_space(Auth, Guid)
            end;
        public ->
            fun file_id:is_share_guid/1
    end,
    lists:all(Predicate, maps:get(<<"file_ids">>, Data)).


-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{gri = #gri{id = undefined, aspect = dir_size_stats_collection_schema, scope = public}}, _) ->
    ok;

validate(#op_req{gri = #gri{id = Guid, aspect = As}}, _) when
    As =:= instance;
    As =:= children;
    As =:= files;
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata;
    As =:= distribution;
    As =:= storage_locations;
    As =:= acl;
    As =:= shares;
    As =:= transfers;
    As =:= qos_summary;
    As =:= dataset_summary;
    As =:= hardlinks;
    As =:= symlink_value;
    As =:= symlink_target;
    As =:= archive_recall_details;
    As =:= archive_recall_progress;
    As =:= archive_recall_log;
    As =:= api_samples;
    element(1, As) =:= dir_size_stats_collection
->
    middleware_utils:assert_file_managed_locally(Guid);

validate(#op_req{gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    middleware_utils:assert_file_managed_locally(FirstGuid),
    middleware_utils:assert_file_managed_locally(SecondGuid);

validate(#op_req{gri = #gri{aspect = download_url}, data = Data}, _) ->
    FileIds = maps:get(<<"file_ids">>, Data),
    lists:foreach(fun(Guid) ->
        middleware_utils:assert_file_managed_locally(Guid)
    end, FileIds).


-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = instance, scope = Sc}}, _) ->
    DefaultAttrs = case Sc of
        private -> ?DEPRECATED_ALL_ATTRS;
        public -> ?DEPRECATED_PUBLIC_ATTRS
    end,
    {AttrType, RequestedAttributes} = infer_requested_attributes(Data, DefaultAttrs),
    {ok, FileAttr} = ?lfm_check(lfm:stat(Auth#auth.session_id, ?FILE_REF(FileGuid), RequestedAttributes)),
    {ok, file_attr_translator:to_json(FileAttr, AttrType, RequestedAttributes)};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = children}}, _) ->
    SessionId = Auth#auth.session_id,
    {AttrType, RequestedAttributes} = infer_requested_attributes(Data, ?DEFAULT_LIST_ATTRS),

    BaseOpts = #{limit => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES)},
    ListingOpts = case maps:get(<<"token">>, Data, undefined) of
        undefined ->
            BaseOpts#{
                offset => maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),
                index => case maps:find(<<"index">>, Data) of
                    {ok, Index} -> file_listing:decode_index(Index);
                    error -> undefined
                end,
                inclusive => maps:get(<<"inclusive">>, Data, true),
                tune_for_large_continuous_listing => maps:get(<<"tuneForLargeContinuousListing">>, Data,
                    maps:get(<<"tune_for_large_continuous_listing">>, Data, false))
            };
        EncodedPaginationToken ->
            BaseOpts#{
                pagination_token => file_listing:decode_pagination_token(EncodedPaginationToken)
            }
    end,

    {ok, ChildrenAttrs, ListingPaginationToken} =
        ?lfm_check(lfm:get_children_attrs(SessionId, ?FILE_REF(FileGuid), ListingOpts, RequestedAttributes)),
    
    {ok, value, {
        lists_utils:pfiltermap(fun(ChildAttr) ->
            {true, file_attr_translator:to_json(ChildAttr, AttrType, RequestedAttributes)}
        end, ChildrenAttrs, ?MAX_MAP_CHILDREN_PROCESSES),
        file_listing:is_finished(ListingPaginationToken), 
        file_listing:encode_pagination_token(ListingPaginationToken)}
    };

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = files}}, _) ->
    SessionId = Auth#auth.session_id,

    ListingOptions = maps_utils:remove_undefined(#{
        limit => maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
        pagination_token => maps:get(<<"token">>, Data, undefined),
        start_after_path => maps:get(<<"start_after">>, Data, undefined),
        prefix => maps:get(<<"prefix">>, Data, undefined),
        include_directories => maps:get(<<"include_directories">>, Data, undefined)
    }),
    {AttrType, RequestedAttributes} = infer_requested_attributes(Data, ?DEFAULT_RECURSIVE_FILE_LIST_ATTRS),
    {ok, Result, InaccessiblePaths, NextPageToken} =
        ?lfm_check(lfm:get_files_recursively(SessionId, ?FILE_REF(FileGuid), ListingOptions, RequestedAttributes)),
    JsonResult = lists_utils:pfiltermap(fun(Attrs) ->
        {true, file_attr_translator:to_json(Attrs, AttrType, RequestedAttributes)}
    end, Result, ?MAX_MAP_CHILDREN_PROCESSES),
    {ok, value, {JsonResult, InaccessiblePaths, NextPageToken}};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = xattrs}}, _) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Inherited = maps:get(<<"inherited">>, Data, false),
    ShowInternal = maps:get(<<"show_internal">>, Data, false),

    case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            {ok, Xattrs} = ?lfm_check(lfm:list_xattr(
                SessionId, FileRef, Inherited, ShowInternal
            )),
            {ok, value, lists:foldl(fun(XattrName, Acc) ->
                {ok, #xattr{value = Value}} = ?lfm_check(lfm:get_xattr(
                    SessionId, FileRef, XattrName, Inherited
                )),
                Acc#{XattrName => Value}
            end, #{}, Xattrs)};
        XattrName ->
            {ok, #xattr{value = Val}} = ?lfm_check(lfm:get_xattr(
                SessionId, FileRef, XattrName, Inherited
            )),
            {ok, value, #{XattrName => Val}}
    end;

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = json_metadata}}, _) ->
    SessionId = Auth#auth.session_id,
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    Inherited = maps:get(<<"inherited">>, Data, false),
    FilterType = maps:get(<<"filter_type">>, Data, undefined),
    Filter = maps:get(<<"filter">>, Data, undefined),

    Query = case {FilterType, Filter} of
        {undefined, _} ->
            [];
        {<<"keypath">>, undefined} ->
            throw(?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>));
        {<<"keypath">>, _} ->
            binary:split(Filter, <<".">>, [global])
    end,

    {ok, value, mi_file_metadata:get_custom_metadata(SessionId, FileRef, json, Query, Inherited)};

get(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = rdf_metadata}}, _) ->
    FileRef = ?FILE_REF(FileGuid, maps:get(<<"resolve_symlink">>, Data, true)),

    {ok, value, mi_file_metadata:get_custom_metadata(Auth#auth.session_id, FileRef, rdf, [], false)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = acl}}, _) ->
    ?lfm_check(lfm:get_acl(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    {ok, mi_file_metadata:gather_distribution(Auth#auth.session_id, ?FILE_REF(FileGuid))};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = storage_locations}}, _) ->
    {ok, mi_file_metadata:get_storage_locations(Auth#auth.session_id, ?FILE_REF(FileGuid))};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = shares}}, _) ->
    {ok, FileAttrs} = ?lfm_check(lfm:stat(Auth#auth.session_id, ?FILE_REF(FileGuid))),
    {ok, FileAttrs#file_attr.shares};

get(#op_req{data = Data, gri = #gri{id = FileGuid, aspect = transfers}}, _) ->
    {ok, #{
        ongoing := Ongoing,
        ended := Ended
    }} = transferred_file:get_transfers(FileGuid),

    Transfers = #{
        <<"ongoingIds">> => Ongoing,
        <<"endedCount">> => length(Ended)
    },
    case maps:get(<<"include_ended_ids">>, Data, false) of
        true ->
            {ok, value, Transfers#{<<"endedIds">> => Ended}};
        false ->
            {ok, value, Transfers}
    end;

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = qos_summary}}, _) ->
    {QosEntriesWithStatus, _AssignedEntries} = mi_qos:get_effective_file_qos(
        Auth#auth.session_id, ?FILE_REF(FileGuid)
    ),
    {ok, #{
        <<"requirements">> => QosEntriesWithStatus,
        <<"status">> => qos_status:aggregate(maps:values(QosEntriesWithStatus))
    }};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = dataset_summary}}, _) ->
    {ok, mi_datasets:get_file_eff_summary(Auth#auth.session_id, ?FILE_REF(FileGuid))};

get(#op_req{auth = Auth, gri = #gri{aspect = download_url}, data = Data}, _) ->
    SessionId = Auth#auth.session_id,
    FileGuids = maps:get(<<"file_ids">>, Data),
    FollowSymlinks = maps:get(<<"follow_symlinks">>, Data, true),
    case page_file_content_download:gen_file_download_url(SessionId, FileGuids, FollowSymlinks) of
        {ok, URL} ->
            {ok, value, URL};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = ?USER(_UserId, SessId), data = Data, gri = #gri{id = FileGuid, aspect = hardlinks}}, _) ->
    {ok, Hardlinks} = Result = ?lfm_check(lfm:get_file_references(
        SessId, ?FILE_REF(FileGuid)
    )),
    case maps:get(<<"limit">>, Data, undefined) of
        undefined ->
            Result;
        Limit ->
            {ok, lists:sublist(Hardlinks, Limit)}
    end;

get(#op_req{gri = #gri{id = FirstGuid, aspect = {hardlinks, SecondGuid}}}, _) ->
    FirstReferencedUuid = fslogic_file_id:ensure_referenced_uuid(file_id:guid_to_uuid(FirstGuid)),
    SecondReferencedUuid = fslogic_file_id:ensure_referenced_uuid(file_id:guid_to_uuid(SecondGuid)),
    case SecondReferencedUuid of
        FirstReferencedUuid -> {ok, #{}};
        _ -> ?ERROR_NOT_FOUND
    end;

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = symlink_value}}, _) ->
    ?lfm_check(lfm:read_symlink(Auth#auth.session_id, ?FILE_REF(FileGuid)));

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = symlink_target, scope = Scope}, data = Data}, _) ->
    SessionId = Auth#auth.session_id,

    {ok, TargetFileGuid} = ?lfm_check(lfm:resolve_symlink(SessionId, ?FILE_REF(FileGuid))),
    
    {AttrType, RequestedAttributes} = infer_requested_attributes(Data, ?API_ATTRS),
    {ok, TargetFileAttrs} = ?lfm_check(lfm:stat(SessionId, ?FILE_REF(TargetFileGuid), RequestedAttributes)),

    TargetFileGri = #gri{
        type = op_file, id = TargetFileGuid,
        aspect = instance, scope = Scope
    },
    {ok, TargetFileGri, file_attr_translator:to_json(TargetFileAttrs, AttrType, RequestedAttributes)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = archive_recall_details}}, _) ->
    {ok, mi_archives:get_recall_details(Auth#auth.session_id, FileGuid)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = archive_recall_progress}}, _) ->
    {ok, mi_archives:get_recall_progress(Auth#auth.session_id, FileGuid)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = archive_recall_log}, data = Data}, _) ->
    BrowseOpts = audit_log_browse_opts:from_json(Data),
    {ok, mi_archives:browse_recall_log(Auth#auth.session_id, FileGuid, BrowseOpts)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = api_samples, scope = public}}, _) ->
    {ok, value, public_file_api_samples:generate_for(Auth#auth.session_id, FileGuid)};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = api_samples, scope = private}}, _) ->
    {ok, value, private_file_api_samples:generate_for(Auth#auth.session_id, FileGuid)};

get(#op_req{gri = #gri{id = undefined, aspect = dir_size_stats_collection_schema}}, _) ->
    {ok, value, ?DIR_SIZE_STATS_COLLECTION_SCHEMA};

get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = {dir_size_stats_collection, ProviderId}}, data = Data}, _) ->
    BrowseRequest = ts_browse_request:from_json(Data),
    {ok, value, mi_file_metadata:get_historical_dir_size_stats(
        Auth#auth.session_id, ?FILE_REF(FileGuid), ProviderId, BrowseRequest)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_listing_start_point_param_spec(binary()) -> middleware_sanitizer:param_spec().
build_listing_start_point_param_spec(Key) ->
    {binary, fun
        (null) ->
            {true, undefined};
        (undefined) ->
            true;
        (<<>>) ->
            throw(?ERROR_BAD_VALUE_EMPTY(Key));
        (Binary) when is_binary(Binary) ->
            true;
        (_) ->
            false
    end}.


%% @private
-spec infer_requested_attributes(middleware:data(), [file_attr:attribute()]) ->
    {file_attr_translator:attr_type() | default, [file_attr:attribute()]}.
infer_requested_attributes(Data, Default) ->
    case maps:find(<<"attributes">>, Data) of
        error ->
            case maps:find(<<"attribute">>, Data) of
                error ->
                    {default, Default};
                {ok, DeprecatedAttrs} ->
                    {deprecated, DeprecatedAttrs}
            end;
        {ok, Attrs} ->
            {current, utils:ensure_list(Attrs)}
    end.
