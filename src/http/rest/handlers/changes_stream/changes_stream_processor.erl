%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Processor for changes stream events.
%%%
%%% This module is responsible for processing changes stream events.
%%% It handles:
%%% - Processing changed documents
%%% - Gathering changes from related documents
%%% - Formatting response data
%%%
%%% For detailed documentation about changes stream functionality,
%%% see changes_stream.hrl.
%%% @end
%%%--------------------------------------------------------------------
-module(changes_stream_processor).
-author("Bartosz Walkowicz").

-include("http/changes_stream.hrl").
-include("middleware/middleware.hrl").

%% API
-export([process_doc/3]).

-type observable_doc_type() :: file_meta | file_location | times | custom_metadata.
-type triggers() :: [observable_doc_type()].

-type doc_monitoring_spec() :: #doc_monitoring_spec{}.
-type changes_monitoring_spec() :: #changes_monitoring_spec{}.

-export_type([
    observable_doc_type/0, triggers/0,
    doc_monitoring_spec/0, changes_monitoring_spec/0
]).

-record(processing_ctx, {
    changed_doc :: datastore:doc(),
    user_ctx :: user_ctx:ctx(),
    file_ctx :: file_ctx:ctx(),
    changes_monitoring_spec :: changes_monitoring_spec(),
    gathered_changes = #{} :: json_utils:json_map()
}).
-type processing_ctx() :: #processing_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec process_doc(user_ctx:ctx(), datastore:doc(), changes_monitoring_spec()) ->
    ok | {ok, json_utils:json_map()}.
process_doc(UserCtx, ChangedDoc, ChangesMonitoringSpec) ->
    ProcessingCtx = #processing_ctx{
        changed_doc = ChangedDoc,
        user_ctx = UserCtx,
        file_ctx = get_file_ctx(ChangedDoc, ChangesMonitoringSpec),
        changes_monitoring_spec = ChangesMonitoringSpec
    },
    case get_all_docs_changes(ProcessingCtx) of
        #processing_ctx{gathered_changes = Changes} when map_size(Changes) == 0 ->
            ok;
        #processing_ctx{file_ctx = FileCtx, gathered_changes = Changes} ->
            CommonInfo = #{
                <<"fileId">> => get_file_object_id(FileCtx),
                <<"filePath">> => get_file_path(FileCtx),
                <<"seq">> => ChangedDoc#document.seq
            },
            {ok, maps:merge(CommonInfo, Changes)}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_file_ctx(datastore:doc(), changes_monitoring_spec()) -> file_ctx:ctx().
get_file_ctx(ChangedDoc = #document{value = #times{}}, ChangesMonitoringSpec) ->
    file_ctx:new_by_uuid(
        ChangedDoc#document.key,
        ChangesMonitoringSpec#changes_monitoring_spec.space_id
    );
get_file_ctx(ChangedDoc = #document{value = #file_meta{}}, ChangesMonitoringSpec) ->
    file_ctx:new_by_doc(
        ChangedDoc,
        ChangesMonitoringSpec#changes_monitoring_spec.space_id
    );
get_file_ctx(ChangedDoc = #document{value = #custom_metadata{}}, ChangesMonitoringSpec) ->
    file_ctx:new_by_uuid(
        ChangedDoc#document.key,
        ChangesMonitoringSpec#changes_monitoring_spec.space_id
    );
get_file_ctx(#document{value = #file_location{uuid = FileUUid}}, ChangesMonitoringSpec) ->
    file_ctx:new_by_uuid(
        FileUUid,
        ChangesMonitoringSpec#changes_monitoring_spec.space_id
    ).


%% @private
-spec get_file_object_id(file_ctx:ctx()) -> file_id:objectid().
get_file_object_id(FileCtx) ->
    try
        {ok, ObjectId} = file_id:guid_to_objectid(file_ctx:get_logical_guid_const(FileCtx)),
        ObjectId
    catch _:Reason ->
        ?debug("Cannot fetch cdmi id for changes, error: ~tp", [Reason]),
        <<>>
    end.


%% @private
-spec get_file_path(file_ctx:ctx()) -> file_meta:path().
get_file_path(FileCtx) ->
    try
        {Path, _} = file_ctx:get_canonical_path(FileCtx),
        Path
    catch _:Reason ->
        ?debug("Cannot fetch Path for changes, error: ~tp", [Reason]),
        <<>>
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves requested fields and metadata from changed document
%% and other documents for which user set `always` flag.
%% @end
%%--------------------------------------------------------------------
-spec get_all_docs_changes(processing_ctx()) -> processing_ctx().
get_all_docs_changes(InitialProcessingCtx = #processing_ctx{
    changes_monitoring_spec = #changes_monitoring_spec{doc_monitoring_specs = DocMonitoringSpecs}
}) ->
    lists:foldl(fun(DocMonitoringSpec, ProcessingCtxAcc) ->
        case is_changed_doc_also_observed_doc(DocMonitoringSpec, ProcessingCtxAcc) of
            true ->
                Doc = ProcessingCtxAcc#processing_ctx.changed_doc,
                get_record_changes(Doc, true, DocMonitoringSpec, ProcessingCtxAcc);
            false ->
                case DocMonitoringSpec#doc_monitoring_spec.always_include_in_other_docs_changes of
                    true ->
                        {Doc, NewProcessingCtxAcc} = get_observed_doc(DocMonitoringSpec, ProcessingCtxAcc),
                        get_record_changes(Doc, false, DocMonitoringSpec, NewProcessingCtxAcc);
                    false ->
                        ProcessingCtxAcc
                end
        end
    end, InitialProcessingCtx, DocMonitoringSpecs).


%% @private
-spec is_changed_doc_also_observed_doc(changes_stream_processor:doc_monitoring_spec(), processing_ctx()) ->
    boolean().
is_changed_doc_also_observed_doc(DocMonitoringSpec, ProcessingCtx) ->
    ObservedDocType = DocMonitoringSpec#doc_monitoring_spec.doc_type,
    ChangedDoc = ProcessingCtx#processing_ctx.changed_doc,

    utils:record_type(ChangedDoc#document.value) == ObservedDocType.


%% @private
-spec get_observed_doc(changes_stream_processor:doc_monitoring_spec(), processing_ctx()) ->
    {datastore:doc(), processing_ctx()}.
get_observed_doc(#doc_monitoring_spec{doc_type = times}, ProcessingCtx) ->
    {ok, Doc} = times:get(get_file_uuid(ProcessingCtx)),
    {Doc, ProcessingCtx};

get_observed_doc(#doc_monitoring_spec{doc_type = file_meta}, ProcessingCtx) ->
    {Doc, NewFileCtx} = file_ctx:get_file_doc(ProcessingCtx#processing_ctx.file_ctx),
    {Doc, ProcessingCtx#processing_ctx{file_ctx = NewFileCtx}};

get_observed_doc(#doc_monitoring_spec{doc_type = file_location}, ProcessingCtx) ->
    {ok, Doc} = file_location:get(get_file_uuid(ProcessingCtx), oneprovider:get_id()),
    {Doc, ProcessingCtx};

get_observed_doc(#doc_monitoring_spec{doc_type = custom_metadata}, ProcessingCtx) ->
    {ok, Doc} = custom_metadata:get(get_file_uuid(ProcessingCtx)),
    {Doc, ProcessingCtx}.


%% @private
-spec get_file_uuid(processing_ctx()) -> file_meta:uuid().
get_file_uuid(#processing_ctx{file_ctx = FileCtx}) ->
    file_ctx:get_logical_uuid_const(FileCtx).


%% @private
-spec get_record_changes(
    datastore:doc(),
    boolean(),
    changes_stream_processor:doc_monitoring_spec(),
    processing_ctx()
) ->
    processing_ctx().
get_record_changes(#document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = #custom_metadata{value = Metadata}
}, IsChangedDoc, DocMonitoringSpec, ProcessingCtx) ->
    Fields = lists:foldl(fun
        (<<"onedata_keyvalue">>, Acc) ->
            KeyValues = case maps:without(?ONEDATA_SPECIAL_XATTRS, Metadata) of
                Map when map_size(Map) == 0 -> null;
                Map -> Map
            end,
            Acc#{<<"onedata_keyvalue">> => KeyValues};
        (FieldName, Acc) ->
            Acc#{FieldName => maps:get(FieldName, Metadata, null)}
    end, #{}, DocMonitoringSpec#doc_monitoring_spec.observed_fields_for_values),

    Exists = lists:foldl(fun
        (<<"onedata_keyvalue">>, Acc) ->
            Acc#{<<"onedata_keyvalue">> => map_size(
                maps:without(?ONEDATA_SPECIAL_XATTRS, Metadata)) > 0
            };
        (FieldName, Acc) ->
            Acc#{FieldName => maps:is_key(FieldName, Metadata)}
    end, #{}, DocMonitoringSpec#doc_monitoring_spec.observed_fields_for_existence),

    update_gathered_changes(DocMonitoringSpec, ProcessingCtx, #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => IsChangedDoc,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields,
        <<"exists">> => Exists
    });

get_record_changes(#document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = #file_meta{} = Record
}, IsChangedDoc, DocMonitoringSpec, ProcessingCtx) ->
    UserCtx = ProcessingCtx#processing_ctx.user_ctx,
    FileCtx = ProcessingCtx#processing_ctx.file_ctx,

    Fields = lists:foldl(fun
        ({<<"name">>, _FieldIndex}, Acc) ->
            {FileName, _NewFileCtx} = file_ctx:get_aliased_name(FileCtx, UserCtx),
            Acc#{<<"name">> => FileName};
        ({FieldName, FieldIndex}, Acc) ->
            Acc#{FieldName => element(FieldIndex, Record)}
    end, #{}, DocMonitoringSpec#doc_monitoring_spec.observed_fields_for_values),

    update_gathered_changes(DocMonitoringSpec, ProcessingCtx, #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => IsChangedDoc,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields
    });

get_record_changes(#document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = Record
}, IsChangedDoc, DocMonitoringSpec, ProcessingCtx) ->
    Fields = lists:foldl(fun({FieldName, FieldIndex}, Acc) ->
        Acc#{FieldName => element(FieldIndex, Record)}
    end, #{}, DocMonitoringSpec#doc_monitoring_spec.observed_fields_for_values),

    update_gathered_changes(DocMonitoringSpec, ProcessingCtx, #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => IsChangedDoc,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields
    }).


%% @private
-spec update_gathered_changes(
    changes_stream_processor:doc_monitoring_spec(),
    processing_ctx(),
    json_utils:json_map()
) ->
    processing_ctx().
update_gathered_changes(DocMonitoringSpec, ProcessingCtx, NewChanges) ->
    DocTypeJson = observed_record_type_to_json(DocMonitoringSpec#doc_monitoring_spec.doc_type),
    ChangesFromOtherDocs = ProcessingCtx#processing_ctx.gathered_changes,
    ProcessingCtx#processing_ctx{gathered_changes = ChangesFromOtherDocs#{DocTypeJson => NewChanges}}.


%% @private
-spec observed_record_type_to_json(observable_doc_type()) -> binary().
observed_record_type_to_json(times) -> <<"times">>;
observed_record_type_to_json(file_meta) -> <<"fileMeta">>;
observed_record_type_to_json(file_location) -> <<"fileLocation">>;
observed_record_type_to_json(custom_metadata) -> <<"customMetadata">>.
