%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for streaming changes happening to `file_meta`, `file_location`,
%%% `times` or `custom_metadata` in scope of given space.
%%%
%%% Possible fields to observer are shown below.
%%%
%%% fileMeta:
%%%     - name
%%%     - type
%%%     - mode
%%%     - owner
%%%     - group_owner
%%%     - provider_id
%%%     - shares
%%%     - deleted
%%%
%%% fileLocation:
%%%     - provider_id
%%%     - storage_id
%%%     - size
%%%     - space_id
%%%     - storage_file_created
%%%
%%% times:
%%%     - atime
%%%     - mtime
%%%     - ctime
%%%
%%% customMetadata:
%%%     - onedata_json
%%%     - onedata_rdf
%%%     - onedata_keyvalue
%%%     ...
%%%
%%% In case of `file_meta`, `file_location` and `times` fields corresponds
%%% one to one to those held in records. As for `custom_metadata` elements
%%% from `value` map field in record can be requested. One additional metakey
%%% named `onedata_keyvalue` can be used to observe all elements beside
%%% `onedata_json` and `onedata_rdf`.
%%%
%%% To open stream user must specify records to observe and either
%%% fields to return (`fields`), fields for which check existence
%%% (`exists` - only possible for `customMetadata`),
%%% whether information about this record should be send always or
%%% on this record changes only (`always` boolean flag with default value
%%% being 'false' - not sending information on other docs changes).
%%%
%%% <record>:
%%%     [fields: <fields>]
%%%     [exists: <fields>]
%%%     [always: boolean()]
%%%
%%% Response will include beside requested information also additional metadata
%%% like fileId, filePath, seq and mutators, rev, deleted, changed for each doc.
%%%
%%% EXAMPLE REQUEST:
%%%
%%% fileMeta:
%%%     fields: [owner]
%%% customMetadata:
%%%     fields: [onedata_rdf]
%%%     exists: [onedata_json]
%%%     always: true
%%%
%%% EXAMPLE RESPONSE:
%%%
%%% fileId: 00000000002C66ED677569642361626562383736303665323765313
%%% filePath: space1/my/file
%%% seq: 100
%%% fileMeta:
%%%     rev: 2-c500a5eb026d9474429903d47841f9c5
%%%     mutators: ["p1.1542789098.test"]
%%%     changed: true
%%%     deleted: false
%%%     fields:
%%%         owner: john
%%% customMetadata:
%%%     rev: 1-09f941b4e8452ef6a244c5181d894814
%%%     mutators: ["p1.1542789098.test"]
%%%     changed: false
%%%     deleted: false
%%%     exists:
%%%         onedata_rdf: true
%%%     fields:
%%%         onedata_json:
%%%             name1: value1
%%%             name2: value2
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(changes).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    terminate/3,
    allowed_methods/2, is_authorized/2,
    content_types_accepted/2
]).

%% resource functions
-export([stream_space_changes/2]).

%% for tests
-export([init_stream/1]).

-record(change_req, {
    record :: file_meta | file_location | times | custom_metadata,
    always = false :: boolean(),
    fields = [] :: [binary()],
    exists = [] :: [binary()]
}).

-type change_req() :: #change_req{}.

-define(DEFAULT_ALWAYS, false).
-define(ONEDATA_SPECIAL_XATTRS, [<<"onedata_json">>, <<"onedata_rdf">>]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, #{changes_stream := Stream, loop_pid := Pid, ref := Ref}) ->
    couchbase_changes:cancel_stream(Stream),
    Pid ! {Ref, stream_ended};
terminate(_, _, #{changes_stream := Stream}) ->
    couchbase_changes:cancel_stream(Stream);
terminate(_, _, #{}) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, stream_space_changes}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/changes/metadata/:sid'
%% @doc
%% This method streams changes happening in space filtering only
%% relevant/requested ones.
%%
%% HTTP method: POST
%%
%% @param timeout Time of inactivity after which close stream.
%% @param last_seq
%%--------------------------------------------------------------------
-spec stream_space_changes(req(), maps:map()) -> {term(), req(), maps:map()}.
stream_space_changes(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_timeout(Req2, State2),
    {State4, Req4} = validator:parse_last_seq(Req3, State3),

    #{auth := Auth, space_id := SpaceId} = State4,
    space_membership:check_with_auth(Auth, SpaceId),

    {ok, Body, Req5} = cowboy_req:read_body(Req4),
    State5 = parse_body(Body, State4),

    State6 = ?MODULE:init_stream(State5),
    Req6 = cowboy_req:stream_reply(
        ?HTTP_OK, #{<<"content-type">> => <<"application/json">>}, Req5
    ),

    ok = stream_loop(Req6, State6),

    cowboy_req:stream_body(<<"">>, fin, Req6),
    {stop, Req6, State6}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec parse_body(binary(), maps:map()) -> maps:map().
parse_body(Body, State) ->
    Json = json_utils:decode(Body),
    case is_map(Json) of
        true -> ok;
        false -> throw(?ERROR_INVALID_CHANGES_REQ)
    end,

    ChangesReqs = maps:fold(fun
        (<<"fileMeta">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = file_meta,
                always = maps:get(<<"always">>, RawSpec, ?DEFAULT_ALWAYS),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"fileLocation">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = file_location,
                always = maps:get(<<"always">>, RawSpec, ?DEFAULT_ALWAYS),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"times">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = times,
                always = maps:get(<<"always">>, RawSpec, ?DEFAULT_ALWAYS),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"customMetadata">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            Fields = maps:get(<<"fields">>, RawSpec, []),
            Exists = maps:get(<<"exists">>, RawSpec, []),
            case {is_list(Fields), is_list(Exists)} of
                {true, true} ->
                    ok;
                {true, false} ->
                    throw(?ERROR_INVALID_FIELD(RecName, <<"exists">>));
                {false, true} ->
                    throw(?ERROR_INVALID_FIELD(RecName, <<"fields">>));
                {false, false} ->
                    throw(?ERROR_INVALID_FIELD(RecName, <<"fields and exists">>))
            end,
            ChangesReq = #change_req{
                record = custom_metadata,
                always = maps:get(<<"always">>, RawSpec, ?DEFAULT_ALWAYS),
                fields = Fields,
                exists = Exists
            },
            [ChangesReq | Acc];
        (RecordName, _, _) ->
            throw(?ERROR_INVALID_FORMAT(RecordName))
    end, [], Json),

    case ChangesReqs of
        [] -> throw(?ERROR_INVALID_CHANGES_REQ);
        _ -> State#{changes_reqs => ChangesReqs}
    end.


%% @private
-spec parse_fields(binary(), term()) -> [{binary(), integer()}].
parse_fields(<<"fileMeta">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_meta_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"fileLocation">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_location_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"times">>, RawFields) when is_list(RawFields) ->
    [{FieldName, times_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(RecName, _RawFields) ->
    throw(?ERROR_INVALID_FORMAT(RecName)).


%% @private
-spec file_meta_field_idx(binary()) -> integer().
file_meta_field_idx(<<"name">>) -> #file_meta.name;
file_meta_field_idx(<<"type">>) -> #file_meta.type;
file_meta_field_idx(<<"mode">>) -> #file_meta.mode;
file_meta_field_idx(<<"owner">>) -> #file_meta.owner;
file_meta_field_idx(<<"group_owner">>) -> #file_meta.group_owner;
file_meta_field_idx(<<"provider_id">>) -> #file_meta.provider_id;
file_meta_field_idx(<<"shares">>) -> #file_meta.shares;
file_meta_field_idx(<<"deleted">>) -> #file_meta.deleted;
file_meta_field_idx(FieldName) ->
    throw(?ERROR_INVALID_FIELD(<<"fileMeta">>, FieldName)).


%% @private
-spec file_location_field_idx(binary()) -> integer().
file_location_field_idx(<<"provider_id">>) -> #file_location.provider_id;
file_location_field_idx(<<"storage_id">>) -> #file_location.storage_id;
file_location_field_idx(<<"size">>) -> #file_location.size;
file_location_field_idx(<<"space_id">>) -> #file_location.space_id;
file_location_field_idx(<<"storage_file_created">>) -> #file_location.storage_file_created;
file_location_field_idx(FieldName) ->
    throw(?ERROR_INVALID_FIELD(<<"fileLocation">>, FieldName)).


%% @private
-spec times_field_idx(binary()) -> integer().
times_field_idx(<<"atime">>) -> #times.atime;
times_field_idx(<<"mtime">>) -> #times.mtime;
times_field_idx(<<"ctime">>) -> #times.ctime;
times_field_idx(FieldName) ->
    throw(?ERROR_INVALID_FIELD(<<"times">>, FieldName)).


%%--------------------------------------------------------------------
%% @doc
%% Init changes stream.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(State :: maps:map()) -> maps:map().
init_stream(State = #{last_seq := Since, space_id := SpaceId}) ->
    ?info("[ changes ]: Starting stream ~p", [Since]),
    Ref = make_ref(),
    Pid = self(),

    % todo limit to admin only (when we will have admin users)
    Node = consistent_hasing:get_node({dbsync_out_stream, SpaceId}),
    {ok, Stream} = rpc:call(Node, couchbase_changes, stream,
        [<<"onedata">>, SpaceId, fun(Feed) ->
            notify(Pid, Ref, Feed)
        end, [{since, Since}], [Pid]]),

    State#{changes_stream => Stream, ref => Ref, loop_pid => Pid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec stream_loop(req(), maps:map()) -> ok | no_return().
stream_loop(Req, State = #{
    timeout := Timeout,
    ref := Ref,
    space_id := SpaceId,
    changes_reqs := ChangesReqs
}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, #document{} = ChangedDoc} ->
            try
                send_change(Req, SpaceId, ChangesReqs, ChangedDoc)
            catch
                _:E ->
                    % Can appear when document connected with deleted file_meta appears
                    ?debug_stacktrace("Cannot stream change of ~p due to: ~p", [
                        ChangedDoc, E
                    ])
            end,
            stream_loop(Req, State)
    after
        Timeout ->
            % TODO VFS-4025 - is it always ok?
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parse and send change received from db stream, to the client.
%% @end
%%--------------------------------------------------------------------
-spec send_change(req(), od_space:id(), [change_req()], datastore:doc()) -> ok.
send_change(Req, SpaceId, ChangesReqs, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #custom_metadata{}
}) ->
    send_changes(Req, Seq, FileUuid, SpaceId, ChangedDoc, ChangesReqs);

send_change(Req, SpaceId, ChangesReqs, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #times{}
}) ->
    send_changes(Req, Seq, FileUuid, SpaceId, ChangedDoc, ChangesReqs);

send_change(Req, SpaceId, ChangesReqs, ChangedDoc = #document{
    seq = Seq,
    value = #file_location{uuid = FileUuid}
}) ->
    send_changes(Req, Seq, FileUuid, SpaceId, ChangedDoc, ChangesReqs);

send_change(Req, SpaceId, ChangesReqs, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #file_meta{}
}) ->
    send_changes(Req, Seq, FileUuid, SpaceId, ChangedDoc, ChangesReqs).


%% @private
-spec send_changes(req(), datastore_doc:seq(), file_meta:uuid(),
    od_space:id(), datastore:doc(), [change_req()]) -> ok.
send_changes(Req, Seq, FileUuid, SpaceId, ChangedDoc, ChangesReqs) ->
    case get_all_docs_changes(ChangesReqs, FileUuid, ChangedDoc) of
        Changes when map_size(Changes) == 0 ->
            ok;
        Changes ->
            FileId =
                try
                    {ok, Val} = cdmi_id:guid_to_objectid(fslogic_uuid:uuid_to_guid(
                        FileUuid, SpaceId
                    )),
                    Val
                catch
                    _:Error1 ->
                        ?debug("Cannot fetch cdmi id for changes, error: ~p", [Error1]),
                        <<>>
                end,
            FilePath =
                try
                    fslogic_uuid:uuid_to_path(?ROOT_SESS_ID, FileUuid)
                catch
                    _:Error2 ->
                        ?debug("Cannot fetch Path for changes, error: ~p", [Error2]),
                        <<>>
                end,
            CommonInfo = #{
                <<"fileId">> => FileId,
                <<"filePath">> => FilePath,
                <<"seq">> => Seq
            },
            Response = json_utils:encode(maps:merge(CommonInfo, Changes)),
            cowboy_req:stream_body(<<Response/binary, "\r\n">>, nofin, Req)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves requested fields and metadata from changed document
%% and other documents for which user set `always` flag.
%% @end
%%--------------------------------------------------------------------
-spec get_all_docs_changes([change_req()], file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_all_docs_changes(ChangesRequests, FileUuid, ChangedDoc) ->
    lists:foldl(fun(ChangesReq, Acc) ->
        maps:merge(Acc, get_requested_changes(ChangesReq, FileUuid, ChangedDoc))
    end, #{}, ChangesRequests).


%% @private
-spec get_requested_changes(change_req(), file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_requested_changes(#change_req{record = times} = Req, FileUuid, ChangedDoc) ->
    get_times_changes(Req, FileUuid, ChangedDoc);
get_requested_changes(#change_req{record = file_meta} = Req, FileUuid, ChangedDoc) ->
    get_file_meta_changes(Req, FileUuid, ChangedDoc);
get_requested_changes(#change_req{record = file_location} = Req, FileUuid, ChangedDoc) ->
    get_file_location_changes(Req, FileUuid, ChangedDoc);
get_requested_changes(#change_req{record = custom_metadata} = Req, FileUuid, ChangedDoc) ->
    get_custom_meta_changes(Req, FileUuid, ChangedDoc).


%% @private
-spec get_times_changes(change_req(), file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_times_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #times{}}) ->
    #{<<"times">> => get_record_changes(true, Fields, Exists, Doc)};
get_times_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _) ->
    {ok, Doc} = times:get(FileUuid),
    #{<<"times">> => get_record_changes(false, Fields, Exists, Doc)};
get_times_changes(_, _, _) ->
    #{}.


%% @private
-spec get_custom_meta_changes(change_req(), file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_custom_meta_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #custom_metadata{}}) ->
    #{<<"customMetadata">> => get_record_changes(true, Fields, Exists, Doc)};
get_custom_meta_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _) ->
    {ok, Doc} = custom_metadata:get(FileUuid),
    #{<<"customMetadata">> => get_record_changes(false, Fields, Exists, Doc)};
get_custom_meta_changes(_, _, _) ->
    #{}.


%% @private
-spec get_file_meta_changes(change_req(), file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_file_meta_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #file_meta{}}) ->
    #{<<"fileMeta">> => get_record_changes(true, Fields, Exists, Doc)};
get_file_meta_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _) ->
    {ok, Doc} = file_meta:get({uuid, FileUuid}),
    #{<<"fileMeta">> => get_record_changes(false, Fields, Exists, Doc)};
get_file_meta_changes(_, _, _) ->
    #{}.


%% @private
-spec get_file_location_changes(change_req(), file_meta:uuid(), datastore:doc()) ->
    maps:map().
get_file_location_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #file_location{}}) ->
    #{<<"fileLocation">> => get_record_changes(true, Fields, Exists, Doc)};
get_file_location_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _) ->
    {ok, Doc} = file_location:get(FileUuid, oneprovider:get_id()),
    #{<<"fileLocation">> => get_record_changes(false, Fields, Exists, Doc)};
get_file_location_changes(_, _, _) ->
    #{}.


%% @private
-spec get_record_changes(boolean(), [term()], [term()], datastore:doc()) ->
    maps:map().
get_record_changes(Changed, FieldsNames, ExistsNames, #document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = #custom_metadata{value = Metadata}
}) ->
    Fields = lists:foldl(fun
        (<<"onedata_keyvalue">>, Acc) ->
            KeyValues = case maps:without(?ONEDATA_SPECIAL_XATTRS, Metadata) of
                Map when map_size(Map) == 0 -> null;
                Map -> Map
            end,
            Acc#{<<"onedata_keyvalue">> => KeyValues};
        (FieldName, Acc) ->
            Acc#{FieldName => maps:get(FieldName, Metadata, null)}
    end, #{}, FieldsNames),

    Exists = lists:foldl(fun
        (<<"onedata_keyvalue">>, Acc) ->
            Acc#{<<"onedata_keyvalue">> => map_size(
                maps:without(?ONEDATA_SPECIAL_XATTRS, Metadata)) > 0
            };
        (FieldName, Acc) ->
            Acc#{FieldName => maps:is_key(FieldName, Metadata)}
    end, #{}, ExistsNames),

    #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => Changed,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields,
        <<"exists">> => Exists
    };

get_record_changes(Changed, FieldsNamesAndIndexes, _Exists, #document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = Record
}) ->
    Fields = lists:foldl(fun({FieldName, FieldIndex}, Acc) ->
        Acc#{FieldName => element(FieldIndex, Record)}
    end, #{}, FieldsNamesAndIndexes),

    #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => Changed,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards changes feed to a streaming process.
%% @end
%%--------------------------------------------------------------------
-spec notify(pid(), reference(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} |
    {error, couchbase_changes:since(), term()}) -> ok.
notify(Pid, Ref, {ok, end_of_stream}) ->
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, {ok, Doc = #document{value = Value}}) when
    is_record(Value, file_meta);
    is_record(Value, custom_metadata);
    is_record(Value, times);
    is_record(Value, file_location) ->
    Pid ! {Ref, Doc},
    ok;
notify(_Pid, _Ref, {ok, #document{}}) ->
    ok;
notify(Pid, Ref, {ok, Docs}) when is_list(Docs) ->
    lists:foreach(fun(Doc) ->
        notify(Pid, Ref, {ok, Doc})
    end, Docs);
notify(Pid, Ref, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~p", [Reason]),
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~p", [Reason]),
    Pid ! {Ref, stream_ended},
    ok.
