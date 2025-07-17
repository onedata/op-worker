%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Parses and validates space files monitoring spec.
%%% @end
%%%--------------------------------------------------------------------
-module(space_files_monitoring_spec).
-author("Bartosz Walkowicz").

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/data_access_control.hrl").

%% API
-export([parse_and_validate/3]).

-type t() :: #space_files_monitoring_spec{}.

-export_type([t/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec parse_and_validate(od_space:id(), session:id(), cowboy_req:req()) ->
    {t(), cowboy_req:req()}.
parse_and_validate(SpaceId, SessionId, Req) ->
    {RawArguments, Req2} = read_arguments(Req),

    ParsedArguments = middleware_sanitizer:sanitize_data(RawArguments, #{
        required => #{
            <<"observedDirectories">> => {list_of_binaries, fun(ObjectIds) ->
                ObjectIds == [] andalso throw(?ERR_BAD_VALUE_EMPTY(?err_ctx(), <<"observedDirectories">>)),

                UserCtx = user_ctx:new(SessionId),
                {true, lists:map(fun({Idx, ObjectId}) ->
                    Key = str_utils:format_bin("observedDirectories[~B]", [Idx]),
                    parse_and_validate_observed_dir(ObjectId, Key, SpaceId, UserCtx)
                end, lists:enumerate(ObjectIds))}
            end}
        },
        optional => #{
            <<"observedAttributes">> => file_middleware_handlers_common_utils:build_attributes_param_spec(
                private, current_events, <<"observedAttributes">>
            )
        }
    }),

    AllObservedAttrs = maps:get(<<"observedAttributes">>, ParsedArguments, ?FILE_META_ATTRS),
    ObservedAttrsPerDoc = lists:foldl(fun({DocType, ObservableAttrs}, Acc) ->
        ObservedDocAttrs = lists_utils:intersect(ObservableAttrs, AllObservedAttrs),
        maps_utils:put_if_defined(Acc, DocType, ObservedDocAttrs, [])
    end, #{}, [
        {file_meta, ?FILE_META_ATTRS},
        {times, ?TIMES_FILE_ATTRS},
        {file_location, ?LOCATION_FILE_ATTRS}
    ]),
    maps_utils:is_empty(ObservedAttrsPerDoc) andalso throw(
        ?ERR_BAD_VALUE_EMPTY(?err_ctx(), <<"observedAttributes">>)
    ),

    FilesMonitoringSpec = #space_files_monitoring_spec{
        observed_dirs = maps:get(<<"observedDirectories">>, ParsedArguments),
        observed_attrs_per_doc = ObservedAttrsPerDoc
    },

    {FilesMonitoringSpec, Req2}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec read_arguments(cowboy_req:req()) -> {json_utils:json_map(), cowboy_req:req()}.
read_arguments(Req) ->
    try
        {ok, Body, Req2} = cowboy_req:read_body(Req),
        ParsedBody = case Body of
            <<"">> -> #{};
            _ -> json_utils:decode(Body)
        end,
        {ParsedBody, Req2}
    catch _:_ ->
        throw(?ERR_MALFORMED_DATA(?err_ctx()))
    end.


%% @private
-spec parse_and_validate_observed_dir(file_id:objectid(), binary(), od_space:id(), user_ctx:ctx()) ->
    file_id:file_guid() | no_return().
parse_and_validate_observed_dir(ObjectId, Key, SpaceId, UserCtx) ->
    FileGuid = middleware_utils:decode_object_id(ObjectId, Key),

    case file_id:guid_to_space_id(FileGuid) of
        SpaceId -> ok;
        _ -> throw(?ERR_BAD_VALUE_IDENTIFIER(?err_ctx(), Key))
    end,

    FileCtx0 = file_ctx:new_by_guid(FileGuid),
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    IsDir orelse throw(?ERR_BAD_DATA(?err_ctx(), Key, ?ERR_POSIX(?err_ctx(), ?ENOTDIR))),

    try
        fslogic_authz:ensure_authorized(
            UserCtx, FileCtx1, [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask)]
        )
    catch
        throw:Errno when is_atom(Errno) ->
            throw(?ERR_BAD_DATA(?err_ctx(), Key, ?ERR_POSIX(?err_ctx(), Errno)));
        Class:Reason:Stacktrace ->
            Error = ?examine_exception(Class, Reason, Stacktrace),
            throw(?ERR_BAD_DATA(?err_ctx(), Key, Error))
    end,

    FileGuid.
