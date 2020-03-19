%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2016-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for streaming changes happening to `file_meta`, `file_location`,
%%% `times` or `custom_metadata` in scope of given space.
%%%
%%% Possible fields to observe are shown below.
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
%%% Optionally `filters` list, that is list of documents whose changes
%%% triggers sending events.
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
%%% filters:
%%%     - fileMeta,
%%%     - times
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
-module(changes_stream_handler).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    init/2, terminate/3,
    allowed_methods/2, is_authorized/2,
    content_types_accepted/2
]).

%% resource functions
-export([stream_space_changes/2]).

%% for tests
-export([init_stream/1]).

-type observable_doc() :: file_meta | file_location | times | custom_metadata.
-type filters() :: [observable_doc()].

-record(change_req, {
    record :: observable_doc(),
    always = false :: boolean(),
    fields = [] :: [binary()],
    exists = [] :: [binary()]
}).
-type change_req() :: #change_req{}.

-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).

-define(DEFAULT_ALWAYS, false).
-define(ONEDATA_SPECIAL_XATTRS, [<<"onedata_json">>, <<"onedata_rdf">>]).

-define(OBSERVABLE_DOCUMENTS, [
    <<"fileMeta">>, <<"fileLocation">>, <<"times">>, <<"customMetadata">>
]).

-define(FILE_META_FIELDS, [
    <<"name">>, <<"type">>, <<"mode">>, <<"owner">>, <<"group_owner">>,
    <<"provider_id">>, <<"shares">>, <<"deleted">>
]).

-define(FILE_LOCATION_FIELDS, [
    <<"provider_id">>, <<"storage_id">>, <<"size">>, <<"space_id">>,
    <<"storage_file_created">>
]).

-define(TIME_FIELDS, [
    <<"atime">>, <<"mtime">>, <<"ctime">>
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Initialize the state for this request.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) ->
    {cowboy_rest, cowboy_req:req(), map()}.
init(Req, _Opts) ->
    {cowboy_rest, Req, #{}}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), cowboy_req:req(), map()) -> ok.
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
-spec allowed_methods(cowboy_req:req(), map() | {error, term()}) ->
    {[binary()], cowboy_req:req(), map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(cowboy_req:req(), map()) ->
    {true | {false, binary()} | halt, cowboy_req:req(), map()}.
is_authorized(Req, State) ->
    case http_auth:authenticate(Req, rest, disallow_data_access_caveats) of
        {ok, ?USER(UserId, SessionId) = Auth} ->
            case authorize(Req, Auth) of
                ok ->
                    {true, Req, State#{user_id => UserId, auth => SessionId}};
                {error, _} = Error ->
                    {stop, send_error_response(Req, Error), State}
            end;
        {ok, ?GUEST} ->
            {stop, cowboy_req:reply(?HTTP_401_UNAUTHORIZED, Req), State};
        {error, _} = Error ->
            {stop, send_error_response(Req, Error), Req}
    end.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_accepted(cowboy_req:req(), map()) ->
    {[{binary(), atom()}], cowboy_req:req(), map()}.
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
-spec stream_space_changes(cowboy_req:req(), map()) ->
    {term(), cowboy_req:req(), map()}.
stream_space_changes(Req, State) ->
    try parse_params(Req, State) of
        {Req2, State2} ->
            State3 = ?MODULE:init_stream(State2),
            Req3 = cowboy_req:stream_reply(
                ?HTTP_200_OK, #{?HDR_CONTENT_TYPE => <<"application/json">>}, Req2
            ),
            stream_loop(Req3, State3),
            cowboy_req:stream_body(<<"">>, fin, Req3),

            {stop, Req3, State3}
    catch
        throw:Error ->
            {stop, send_error_response(Req, Error), State};
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:process_request - ~p:~p", [
                ?MODULE, Type, Message
            ]),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, State}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authorize(cowboy_req:req(), aai:auth()) -> ok | errors:error().
authorize(Req, ?USER(UserId) = Auth) ->
    SpaceId = cowboy_req:binding(sid, Req),

    case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_CHANGES_STREAM) of
        true ->
            try
                GRI = #gri{type = op_metrics, id = SpaceId, aspect = changes},
                api_auth:check_authorization(Auth, ?OP_WORKER, create, GRI)
            catch
                _:_ ->
                    ?ERROR_INTERNAL_SERVER_ERROR
            end;
        false ->
            ?ERROR_FORBIDDEN
    end.


%% @private
-spec send_error_response(cowboy_req:req(), errors:error()) -> cowboy_req:req().
send_error_response(Req, Error) ->
    #rest_resp{
        code = Code,
        headers = Headers,
        body = Body
    } = rest_translator:error_response(Error),
    cowboy_req:reply(Code, Headers, json_utils:encode(Body), Req).


%% @private
-spec parse_params(cowboy_req:req(), map()) -> {cowboy_req:req(), map()}.
parse_params(Req, State0) ->
    SpaceId = cowboy_req:binding(sid, Req),
    QueryParams = maps:from_list(cowboy_req:parse_qs(Req)),
    State1 = State0#{
        space_id => SpaceId,
        timeout => parse_timeout(QueryParams),
        last_seq => parse_last_seq(SpaceId, QueryParams)
    },

    {ok, Body, Req2} = cowboy_req:read_body(Req),
    State2 = parse_body(Body, State1),

    {Req2, State2}.


%% @private
-spec parse_timeout(map()) -> infinity | integer() | no_return().
parse_timeout(Params) ->
    case maps:get(<<"timeout">>, Params, ?DEFAULT_TIMEOUT) of
        <<"infinity">> ->
            infinity;
        NumberBin ->
            parse_integer(<<"timeout">>, NumberBin)
    end.


%% @private
-spec parse_last_seq(od_space:id(), map()) -> null | integer() | no_return().
parse_last_seq(SpaceId, Params) ->
    case maps:get(<<"last_seq">>, Params, ?DEFAULT_LAST_SEQ) of
        <<"now">> ->
            dbsync_state:get_seq(SpaceId, oneprovider:get_id());
        NumberBin ->
            parse_integer(<<"last_seq">>, NumberBin)
    end.


%% @private
-spec parse_integer(binary(), binary()) -> integer() | no_return().
parse_integer(Param, ValueBin) ->
    try
        binary_to_integer(ValueBin)
    catch _:_ ->
        throw(?ERROR_BAD_VALUE_INTEGER(Param))
    end.


%% @private
-spec parse_body(binary(), map()) -> map() | no_return().
parse_body(Body, State) ->
    ChangesSpecification = json_utils:decode(Body),
    case is_map(ChangesSpecification) of
        true -> ok;
        false -> throw(?ERROR_BAD_VALUE_JSON(<<"changesSpecification">>))
    end,

    Filters = parse_filters(ChangesSpecification),

    ChangesReqs = maps:fold(fun
        (<<"fileMeta">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = file_meta,
                always = parse_always_flag(RecName, RawSpec),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"fileLocation">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = file_location,
                always = parse_always_flag(RecName, RawSpec),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"times">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            ChangesReq = #change_req{
                record = times,
                always = parse_always_flag(RecName, RawSpec),
                fields = parse_fields(RecName, maps:get(<<"fields">>, RawSpec, []))
            },
            [ChangesReq | Acc];
        (<<"customMetadata">> = RecName, RawSpec, Acc) when is_map(RawSpec) ->
            Fields = maps:get(<<"fields">>, RawSpec, []),
            Exists = maps:get(<<"exists">>, RawSpec, []),
            case {is_list(Fields), is_list(Exists)} of
                {true, true} ->
                    ok;
                {false, _} ->
                    throw(?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<RecName/binary, ".fields">>));
                {_, false} ->
                    throw(?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<RecName/binary, ".exists">>))
            end,
            ChangesReq = #change_req{
                record = custom_metadata,
                always = parse_always_flag(RecName, RawSpec),
                fields = Fields,
                exists = Exists
            },
            [ChangesReq | Acc];
        (RecordName, _, _) ->
            throw(?ERROR_BAD_DATA(RecordName))
    end, [], ChangesSpecification),

    case ChangesReqs of
        [] ->
            throw(?ERROR_MISSING_AT_LEAST_ONE_VALUE(?OBSERVABLE_DOCUMENTS));
        _ ->
            State#{
                filters => Filters,
                changes_reqs => ChangesReqs
            }
    end.


%% @private
-spec parse_filters(map()) -> filters() | no_return().
parse_filters(ChangesSpecification) ->
    RawFilters = case maps:get(<<"filters">>, ChangesSpecification, undefined) of
        undefined ->
            ?OBSERVABLE_DOCUMENTS;
        FiltersSpec when is_list(FiltersSpec) ->
            FiltersSpec;
        _ ->
            throw(?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"filters">>))
    end,

    lists:usort(lists:map(fun
        (<<"fileMeta">>) ->
            file_meta;
        (<<"fileLocation">>) ->
            file_location;
        (<<"times">>) ->
            times;
        (<<"customMetadata">>) ->
            custom_metadata;
        (_) ->
            throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filters">>, ?OBSERVABLE_DOCUMENTS))
    end, RawFilters)).


%% @private
-spec parse_always_flag(binary(), map()) -> true | false | no_return().
parse_always_flag(RecName, Spec) ->
    case maps:get(<<"always">>, Spec, ?DEFAULT_ALWAYS) of
        true ->
            true;
        false ->
            false;
        _ ->
            throw(?ERROR_BAD_VALUE_BOOLEAN(<<RecName/binary, ".always">>))
    end.


%% @private
-spec parse_fields(binary(), term()) -> [{binary(), integer()}] | no_return().
parse_fields(<<"fileMeta">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_meta_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"fileLocation">>, RawFields) when is_list(RawFields) ->
    [{FieldName, file_location_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(<<"times">>, RawFields) when is_list(RawFields) ->
    [{FieldName, times_field_idx(FieldName)} || FieldName <- RawFields];
parse_fields(RecName, _RawFields) ->
    throw(?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<RecName/binary, ".fields">>)).


%% @private
-spec file_meta_field_idx(binary()) -> integer() | no_return().
file_meta_field_idx(<<"name">>) -> #file_meta.name;
file_meta_field_idx(<<"type">>) -> #file_meta.type;
file_meta_field_idx(<<"mode">>) -> #file_meta.mode;
file_meta_field_idx(<<"owner">>) -> #file_meta.owner;
file_meta_field_idx(<<"group_owner">>) -> #file_meta.group_owner;
file_meta_field_idx(<<"provider_id">>) -> #file_meta.provider_id;
file_meta_field_idx(<<"shares">>) -> #file_meta.shares;
file_meta_field_idx(<<"deleted">>) -> #file_meta.deleted;
file_meta_field_idx(_FieldName) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"fileMeta.fields">>, ?FILE_META_FIELDS)).


%% @private
-spec file_location_field_idx(binary()) -> integer() | no_return().
file_location_field_idx(<<"provider_id">>) -> #file_location.provider_id;
file_location_field_idx(<<"storage_id">>) -> #file_location.storage_id;
file_location_field_idx(<<"size">>) -> #file_location.size;
file_location_field_idx(<<"space_id">>) -> #file_location.space_id;
file_location_field_idx(<<"storage_file_created">>) -> #file_location.storage_file_created;
file_location_field_idx(_FieldName) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"fileLocation.fields">>, ?FILE_LOCATION_FIELDS)).


%% @private
-spec times_field_idx(binary()) -> integer() | no_return().
times_field_idx(<<"atime">>) -> #times.atime;
times_field_idx(<<"mtime">>) -> #times.mtime;
times_field_idx(<<"ctime">>) -> #times.ctime;
times_field_idx(_FieldName) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"times.fields">>, ?TIME_FIELDS)).


%%--------------------------------------------------------------------
%% @doc
%% Init changes stream.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(State :: map()) -> map().
init_stream(#{last_seq := Since, space_id := SpaceId, filters := Filters} = State) ->
    ?info("[ changes ]: Starting stream ~p", [Since]),
    Ref = make_ref(),
    Pid = self(),

    % TODO VFS-5570
    Node = datastore_key:responsible_node(SpaceId),
    {ok, Stream} = rpc:call(Node, couchbase_changes, stream, [
        <<"onedata">>,
        SpaceId,
        fun(Feed) -> notify(Pid, Ref, Filters, Feed) end,
        [{since, Since}],
        [Pid]
    ]),

    State#{changes_stream => Stream, ref => Ref, loop_pid => Pid}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec stream_loop(cowboy_req:req(), map()) -> ok.
stream_loop(Req, State = #{
    changes_stream := Stream,
    timeout := Timeout,
    ref := Ref
}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, ChangedDocs} when is_list(ChangedDocs) ->
            Stream ! {Ref, ok},
            lists:foreach(fun(ChangedDoc) ->
                try
                    send_change(Req, ChangedDoc, State)
                catch Type:Reason ->
                    % Can appear when document connected with deleted file_meta appears
                    ?debug_stacktrace("Cannot stream change of ~p due to ~p:~p", [
                        ChangedDoc, Type, Reason
                    ])
                end
            end, ChangedDocs),
            stream_loop(Req, State);
        Msg ->
            ?log_bad_request(Msg),
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
-spec send_change(cowboy_req:req(), datastore:doc(), map()) -> ok.
send_change(Req, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #custom_metadata{}
}, State) ->
    send_changes(Req, Seq, FileUuid, ChangedDoc, State);

send_change(Req, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #times{}
}, State) ->
    send_changes(Req, Seq, FileUuid, ChangedDoc, State);

send_change(Req, ChangedDoc = #document{
    seq = Seq,
    value = #file_location{uuid = FileUuid}
}, State) ->
    send_changes(Req, Seq, FileUuid, ChangedDoc, State);

send_change(Req, ChangedDoc = #document{
    seq = Seq,
    key = FileUuid,
    value = #file_meta{}
}, State) ->
    send_changes(Req, Seq, FileUuid, ChangedDoc, State).


%% @private
-spec send_changes(cowboy_req:req(), datastore_doc:seq(), file_meta:uuid(),
    datastore:doc(), map()) -> ok.
send_changes(Req, Seq, FileUuid, ChangedDoc, State) ->
    case get_all_docs_changes(FileUuid, ChangedDoc, State) of
        Changes when map_size(Changes) == 0 ->
            ok;
        Changes ->
            FileId =
                try
                    {ok, Val} = file_id:guid_to_objectid(file_id:pack_guid(
                        FileUuid, maps:get(space_id, State)
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
-spec get_all_docs_changes(file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_all_docs_changes(FileUuid, ChangedDoc, State) ->
    lists:foldl(fun(ChangesReq, Acc) ->
        maps:merge(Acc, get_requested_changes(ChangesReq, FileUuid, ChangedDoc, State))
    end, #{}, maps:get(changes_reqs, State)).


%% @private
-spec get_requested_changes(change_req(), file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_requested_changes(#change_req{record = times} = Req, FileUuid, ChangedDoc, State) ->
    get_times_changes(Req, FileUuid, ChangedDoc, State);
get_requested_changes(#change_req{record = file_meta} = Req, FileUuid, ChangedDoc, State) ->
    get_file_meta_changes(Req, FileUuid, ChangedDoc, State);
get_requested_changes(#change_req{record = file_location} = Req, FileUuid, ChangedDoc, State) ->
    get_file_location_changes(Req, FileUuid, ChangedDoc, State);
get_requested_changes(#change_req{record = custom_metadata} = Req, FileUuid, ChangedDoc, State) ->
    get_custom_meta_changes(Req, FileUuid, ChangedDoc, State).


%% @private
-spec get_times_changes(change_req(), file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_times_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #times{}}, State) ->
    #{<<"times">> => get_record_changes(true, Fields, Exists, Doc, State)};
get_times_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _, State) ->
    {ok, Doc} = times:get(FileUuid),
    #{<<"times">> => get_record_changes(false, Fields, Exists, Doc, State)};
get_times_changes(_, _, _, _) ->
    #{}.


%% @private
-spec get_custom_meta_changes(change_req(), file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_custom_meta_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #custom_metadata{}}, State) ->
    #{<<"customMetadata">> => get_record_changes(true, Fields, Exists, Doc, State)};
get_custom_meta_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _, State) ->
    {ok, Doc} = custom_metadata:get(FileUuid),
    #{<<"customMetadata">> => get_record_changes(false, Fields, Exists, Doc, State)};
get_custom_meta_changes(_, _, _, _) ->
    #{}.


%% @private
-spec get_file_meta_changes(change_req(), file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_file_meta_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #file_meta{}}, State) ->
    #{<<"fileMeta">> => get_record_changes(true, Fields, Exists, Doc, State)};
get_file_meta_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _, State) ->
    {ok, Doc} = file_meta:get({uuid, FileUuid}),
    #{<<"fileMeta">> => get_record_changes(false, Fields, Exists, Doc, State)};
get_file_meta_changes(_, _, _, _) ->
    #{}.


%% @private
-spec get_file_location_changes(change_req(), file_meta:uuid(), datastore:doc(), map()) ->
    map().
get_file_location_changes(#change_req{
    fields = Fields,
    exists = Exists
}, _, Doc = #document{value = #file_location{}}, State) ->
    #{<<"fileLocation">> => get_record_changes(true, Fields, Exists, Doc, State)};
get_file_location_changes(#change_req{
    always = true,
    fields = Fields,
    exists = Exists
}, FileUuid, _, State) ->
    {ok, Doc} = file_location:get(FileUuid, oneprovider:get_id()),
    #{<<"fileLocation">> => get_record_changes(false, Fields, Exists, Doc, State)};
get_file_location_changes(_, _, _, _) ->
    #{}.


%% @private
-spec get_record_changes(boolean(), [term()], [term()], datastore:doc(), map()) ->
    map().
get_record_changes(Changed, FieldsNames, ExistsNames, #document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = #custom_metadata{value = Metadata}
}, _State) ->
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

get_record_changes(Changed, FieldsNamesAndIndices, _Exists, #document{
    key = FileUuid,
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = #file_meta{} = Record
}, State) ->
    Fields = lists:foldl(
        fun
            ({<<"name">>, _FieldIndex}, Acc) ->
                Auth = maps:get(auth, State),
                SpaceId = maps:get(space_id, State),
                SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                Name = case FileUuid =:= SpaceUuid of
                    true ->
                        {ok, SpaceName} = space_logic:get_name(Auth, SpaceId),
                        SpaceName;
                    false ->
                        Record#file_meta.name
                end,
                Acc#{<<"name">> => Name};
            ({FieldName, FieldIndex}, Acc) ->
                Acc#{FieldName => element(FieldIndex, Record)}
        end, #{}, FieldsNamesAndIndices),

    #{
        <<"rev">> => Rev,
        <<"mutators">> => Mutators,
        <<"changed">> => Changed,
        <<"deleted">> => Deleted,
        <<"fields">> => Fields
    };

get_record_changes(Changed, FieldsNamesAndIndices, _Exists, #document{
    revs = [Rev | _],
    mutators = Mutators,
    deleted = Deleted,
    value = Record
}, _State) ->
    Fields = lists:foldl(fun({FieldName, FieldIndex}, Acc) ->
        Acc#{FieldName => element(FieldIndex, Record)}
    end, #{}, FieldsNamesAndIndices),

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
-spec notify(pid(), reference(), filters(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} |
    {error, couchbase_changes:since(), term()}) -> ok.
notify(Pid, Ref, Filters, {ok, #document{} = Doc}) ->
    case is_observed_doc(Doc, Filters) of
        true ->
            call_changes_stream_handler(Pid, Ref, [Doc]);
        false ->
            ok
    end,
    ok;
notify(Pid, Ref, Filters, {ok, Docs}) when is_list(Docs) ->
    case lists:filter(fun(Doc) -> is_observed_doc(Doc, Filters) end, Docs) of
        [] ->
            ok;
        RelevantDocs ->
            call_changes_stream_handler(Pid, Ref, RelevantDocs)
    end,
    ok;
notify(Pid, Ref, _Filters, {ok, end_of_stream}) ->
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, _Filters, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~p", [Reason]),
    Pid ! {Ref, stream_ended},
    ok;
notify(Pid, Ref, _Filters, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~p", [Reason]),
    Pid ! {Ref, stream_ended},
    ok.


%% @private
-spec is_observed_doc(datastore:doc(), filters()) -> boolean().
is_observed_doc(#document{value = #file_meta{}}, Filters) ->
    lists:member(file_meta, Filters);
is_observed_doc(#document{value = #custom_metadata{}}, Filters) ->
    lists:member(custom_metadata, Filters);
is_observed_doc(#document{value = #times{}}, Filters) ->
    lists:member(times, Filters);
is_observed_doc(#document{value = #file_location{}}, Filters) ->
    lists:member(file_location, Filters);
is_observed_doc(_Doc, _Filters) ->
    false.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Send synchronous message to changes_stream_handler and await confirmation
%% that msg was received.
%% @end
%%--------------------------------------------------------------------
call_changes_stream_handler(Pid, Ref, Msg) ->
    Pid ! {Ref, Msg},
    receive
        {Ref, ok} ->
            ok
    end,
    ok.
