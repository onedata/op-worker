%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Prepare answer for cdmi object request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_object_answer).
-author("Tomasz Lichon").

-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([prepare/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%%--------------------------------------------------------------------
-spec prepare([FieldName :: binary()], #{}) ->
    [{FieldName :: binary(), Value :: term()}].
prepare([], _State) ->
    [];
prepare([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-object">>} | prepare(Tail, State)];
prepare([<<"objectID">> | Tail], #{attributes := #file_attr{guid = Uuid}} = State) ->
    {ok, Id} = cdmi_id:uuid_to_objectid(Uuid),
    [{<<"objectID">>, Id} | prepare(Tail, State)];
prepare([<<"objectName">> | Tail], #{path := Path} = State) ->
    [{<<"objectName">>, filename:basename(Path)} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := <<"/">>} = State) ->
    [{<<"parentURI">>, <<>>} | prepare(Tail, State)];
prepare([<<"parentURI">> | Tail], #{path := Path} = State) ->
    ParentURI = filepath_utils:parent_dir(Path),
    [{<<"parentURI">>, ParentURI} | prepare(Tail, State)];
prepare([<<"parentID">> | Tail], #{path := <<"/">>} = State) ->
    [{<<"parentID">>, <<>>} | prepare(Tail, State)];
prepare([<<"parentID">> | Tail], #{path := Path, auth := Auth} = State) ->
    {ok, #file_attr{guid = Uuid}} =
        onedata_file_api:stat(Auth, {path, filename:dirname(Path)}),
    {ok, Id} = cdmi_id:uuid_to_objectid(Uuid),
    [{<<"parentID">>, Id} | prepare(Tail, State)];
prepare([<<"capabilitiesURI">> | Tail], State) ->
    [{<<"capabilitiesURI">>, ?dataobject_capability_path} | prepare(Tail, State)];
prepare([<<"completionStatus">> | Tail], #{auth := Auth, attributes := #file_attr{guid = Uuid}} = State) ->
    CompletionStatus = cdmi_metadata:get_cdmi_completion_status(Auth, {guid, Uuid}),
    [{<<"completionStatus">>, CompletionStatus} | prepare(Tail, State)];
prepare([<<"mimetype">> | Tail], #{auth := Auth, attributes := #file_attr{guid = Uuid}} = State) ->
    Mimetype = cdmi_metadata:get_mimetype(Auth, {guid, Uuid}),
    [{<<"mimetype">>, Mimetype} | prepare(Tail, State)];
prepare([<<"metadata">> | Tail], #{auth := Auth, attributes := Attrs = #file_attr{guid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {guid, Uuid}, Attrs)} | prepare(Tail, State)];
prepare([{<<"metadata">>, Prefix} | Tail], #{auth := Auth, attributes := Attrs = #file_attr{guid = Uuid}} = State) ->
    [{<<"metadata">>, cdmi_metadata:prepare_metadata(Auth, {guid, Uuid}, Prefix, Attrs)} | prepare(Tail, State)];
prepare([<<"valuetransferencoding">> | Tail], #{auth := Auth, attributes := #file_attr{guid = Uuid}} = State) ->
    Encoding = cdmi_metadata:get_encoding(Auth, {guid, Uuid}),
    [{<<"valuetransferencoding">>, Encoding} | prepare(Tail, State)];
prepare([<<"value">> | Tail], State) ->
    [{<<"value">>, {range, default}} | prepare(Tail, State)];
prepare([{<<"value">>, From, To} | Tail], State) ->
    [{<<"value">>, {range, {From, To}}} | prepare(Tail, State)];
prepare([<<"valuerange">> | Tail], #{options := Opts, attributes := Attrs} = State) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From, To} ->
            [{<<"valuerange">>, iolist_to_binary([integer_to_binary(From), <<"-">>, integer_to_binary(To)])} | prepare(Tail, State)];
        _ ->
            [{<<"valuerange">>, iolist_to_binary([<<"0-">>, integer_to_binary(Attrs#file_attr.size - 1)])} | prepare(Tail, State)] %todo fix 0--1 when file is empty
    end;
prepare([_Other | Tail], State) ->
    prepare(Tail, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================