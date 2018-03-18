%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for changes streaming
%%% @end
%%%--------------------------------------------------------------------
-module(changes).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([get_space_changes/2]).

-record(change, {
    seq :: non_neg_integer(),
    doc :: datastore:doc(),
    model :: datastore_model:model()
}).

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
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_space_changes}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method returns selected file attributes.
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Type of attribute to query for.
%%--------------------------------------------------------------------
-spec get_space_changes(req(), maps:map()) -> {term(), req(), maps:map()}.
get_space_changes(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_timeout(Req2, State2),
    {State4, Req4} = validator:parse_last_seq(Req3, State3),

    #{auth := Auth, space_id := SpaceId} = State4,

    space_membership:check_with_auth(Auth, SpaceId),
    State5 = init_stream(State4),

    Req5 = cowboy_req:stream_reply(
        ?HTTP_OK, #{<<"content-type">> => <<"application/json">>}, Req4
    ),
    ok = stream_loop(Req5, State5),
    cowboy_req:stream_body(<<"">>, fin, Req),
    {stop, Req5, State5}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
        end, [{since, Since}]]),

    State#{changes_stream => Stream, ref => Ref, loop_pid => Pid}.

%%--------------------------------------------------------------------
%% @doc
%% Listens for events and pushes them to the socket
%% @end
%%--------------------------------------------------------------------
-spec stream_loop(req(), maps:map()) -> ok | no_return().
stream_loop(Req, State = #{timeout := Timeout, ref := Ref, space_id := SpaceId}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, Change} ->
            try
                send_change(Req, Change, SpaceId)
            catch
                _:E ->
                    ?error_stacktrace("Cannot stream change of ~p due to: ~p", [Change, E])
            end,
            stream_loop(Req, State)
    after
        Timeout ->
            % TODO VFS-4025 - is it always ok?
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Parse and send change received from db stream, to the client.
%%--------------------------------------------------------------------
-spec send_change(req(), #change{}, od_space:id()) -> ok.
send_change(Req, #change{seq = Seq, doc = #document{
    key = FileUuid, value = #custom_metadata{}}}, RequestedSpaceId) ->
    {ok, FileDoc} = file_meta:get({uuid, FileUuid}),
    send_change(Req, #change{seq = Seq, doc = FileDoc, model = file_meta},
        RequestedSpaceId);
send_change(Req, #change{seq = Seq, doc = #document{
    key = FileUuid, value = #times{}}}, RequestedSpaceId) ->
    {ok, FileDoc} = file_meta:get({uuid, FileUuid}),
    send_change(Req, #change{seq = Seq, doc = FileDoc, model = file_meta},
        RequestedSpaceId);
send_change(Req, #change{seq = Seq, doc = #document{
    value = #file_location{uuid = FileUuid, provider_id = ProviderId}}}, RequestedSpaceId) ->
    case oneprovider:is_self(ProviderId) of
        true ->
            {ok, FileDoc} = file_meta:get({uuid, FileUuid}),
            send_change(Req, #change{seq = Seq, doc = FileDoc, model = file_meta},
                RequestedSpaceId);
        false ->
            ok
    end;
send_change(Req, Change, RequestedSpaceId) ->
    SpaceDirUuid =
        case Change#change.doc#document.value#file_meta.is_scope of
            true ->
                Change#change.doc#document.key;
            false ->
                Change#change.doc#document.value#file_meta.scope
        end,
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirUuid),

    case SpaceId =:= RequestedSpaceId of
        true ->
            Json = prepare_response(Change, SpaceId),
            cowboy_req:stream_body(<<Json/binary, "\r\n">>, nofin, Req);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Prepare response in json format
%%--------------------------------------------------------------------
-spec prepare_response(#change{}, SpaceId :: binary()) -> binary().
prepare_response(#change{seq = Seq, doc = FileDoc = #document{
    key = Uuid, deleted = Deleted,
    value = #file_meta{
        is_scope = IsScope, mode = Mode, type = Type, owner = Uid,
        version = Version, name = Name}}}, SpaceId) ->
    #times{atime = Atime, ctime = Ctime, mtime = Mtime} =
        try
            {ok, #document{value = TimesValue}} = times:get(Uuid),
            TimesValue
        catch
            _:Error0 ->
                ?error("Cannot fetch times for changes, error: ~p", [Error0]),
                #times{}
        end,
    Guid =
        try
            {ok, Val} = cdmi_id:guid_to_objectid(fslogic_uuid:uuid_to_guid(Uuid, SpaceId)),
            Val
        catch
            _:Error1 ->
                ?error("Cannot fetch guid for changes, error: ~p", [Error1]),
                <<>>
        end,
    Path =
        try
            fslogic_uuid:uuid_to_path(?ROOT_SESS_ID, Uuid)
        catch
            _:Error2 ->
                ?error("Cannot fetch Path for changes, error: ~p", [Error2]),
                <<>>
        end,
    Xattrs =
        try
            case custom_metadata:get(Uuid) of
                {ok, #document{value = #custom_metadata{value = Metadata}}} ->
                    Metadata;
                {error, not_found} ->
                    #{}
            end
        catch
            _:Error3 ->
                ?error("Cannot fetch xattrs for changes, error: ~p", [Error3]),
                <<"fetching_error">>
        end,
    Size =
        try
            FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
            {FileSize, _FileCtx2} = file_ctx:get_file_size(FileCtx),
            FileSize
        catch
            _:Error4 ->
                ?error("Cannot fetch size for changes, error: ~p", [Error4]),
                <<"fetching_error">>
        end,

    Response =
        #{
            <<"changes">> => #{
                <<"atime">> => Atime,
                <<"ctime">> => Ctime,
                <<"is_scope">> => IsScope,
                <<"mode">> => Mode,
                <<"mtime">> => Mtime,
                <<"scope">> => SpaceId,
                <<"size">> => Size,
                <<"type">> => Type,
                <<"uid">> => Uid,
                <<"version">> => Version,
                <<"xattrs">> => Xattrs
            },
            <<"deleted">> => Deleted,
            <<"file_id">> => Guid,
            <<"file_path">> => Path,
            <<"name">> => Name,
            <<"seq">> => Seq
        },
    json_utils:encode_map(Response).


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
notify(Pid, Ref, {ok, Doc = #document{seq = Seq, value = Value}}) when
    is_record(Value, file_meta);
    is_record(Value, custom_metadata);
    is_record(Value, times);
    is_record(Value, file_location) ->
    Pid ! {Ref, #change{seq = Seq, doc = Doc, model = element(1, Value)}},
    ok;
notify(_Pid, _Ref, {ok, #document{}}) ->
    ok;
notify(Pid, Ref, {ok, Docs}) when is_list(Docs) ->
    lists:foreach(fun(Doc) ->
        notify(Pid, Ref, {ok, Doc})
    end, Docs);
notify(Pid, Ref, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~p", [Reason]),
    Pid ! {Ref, stream_ended},
    ok.