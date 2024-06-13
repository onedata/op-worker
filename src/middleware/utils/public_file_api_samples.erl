%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API samples for publicly available (shared) files.
%%% API samples are generated for each file separately and they include
%%% information specific for the file, e.g. its Id. The list of returned
%%% operations depends on the file type.
%%%
%%% Two types of APIs are supported; REST and xrootd (only if
%%% "openDataXrootdServerDomain" config variable is set in Onezone).
%%%
%%% NOTE: API samples do not cover all available API, only the commonly used
%%% endpoints and options.
%%% @end
%%%-------------------------------------------------------------------
-module(public_file_api_samples).
-author("Lukasz Opiola").

-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/api_samples/common.hrl").


-export([generate_for/2]).


-define(XROOTD_URI(Domain, Path), str_utils:format_bin("root://~ts~ts", [Domain, Path])).


%%%===================================================================
%%% API
%%%===================================================================


-spec generate_for(session:id(), fslogic_worker:file_guid()) -> json_utils:json_term().
generate_for(SessionId, FileGuid) ->
    {ok, #file_attr{type = FileType}} = ?lfm_check(lfm:stat(SessionId, ?FILE_REF(FileGuid))),
    {ok, FileId} = file_id:guid_to_objectid(FileGuid),
    XRootDApiTemplates = case lookup_open_data_xrootd_server_domain() of
        false ->
            #{};
        {true, XRootDDomain} ->
            {_, SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),
            FilePath = resolve_file_path_in_share(SessionId, FileGuid),
            #{<<"xrootd">> => xrootd_api(FileType, XRootDDomain, SpaceId, ShareId, FilePath)}
    end,
    XRootDApiTemplates#{
        <<"rest">> => jsonable_record:to_json(rest_api(FileType, FileId), rest_api_samples)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec lookup_open_data_xrootd_server_domain() -> false | {true, binary()}.
lookup_open_data_xrootd_server_domain() ->
    {ok, OnezoneConfiguration} = provider_logic:get_service_configuration(onezone),
    case maps:get(<<"openDataXrootdServerDomain">>, OnezoneConfiguration, null) of
        null -> false;
        XRootDDomain -> {true, XRootDDomain}
    end.


%% @private
-spec resolve_file_path_in_share(session:id(), fslogic_worker:file_guid()) -> file_meta:path().
resolve_file_path_in_share(SessionId, ShareGuid) ->
    {ok, #file_attr{
        name = Name,
        parent_guid = ParentShareGuid
    }} = ?lfm_check(lfm:stat(SessionId, ?FILE_REF(ShareGuid))),
    case ParentShareGuid of
        undefined ->
            filename:join([?DIRECTORY_SEPARATOR, Name]);
        _ ->
            PathToParent = resolve_file_path_in_share(SessionId, ParentShareGuid),
            filename:join([PathToParent, Name])
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% NOTE: Since there is only a couple of XRootD commands and the XRootD config
%% is not set in typical deployments, these commands have simplified structure and
%% are prepared in a JSON-ready format.
%% @end
%%--------------------------------------------------------------------
-spec xrootd_api(onedata_file:type(), binary(), od_space:id(), od_share:id(), file_meta:path()) ->
    [json_utils:json_map()].
xrootd_api(?DIRECTORY_TYPE, Domain, SpaceId, ShareId, FilePath) ->
    FullDataPath = str_utils:format_bin("/data/~ts/~ts/~ts~ts", [SpaceId, SpaceId, ShareId, FilePath]),
    [
        #{
            <<"name">> => <<"Download directory (recursively)">>,
            <<"description">> => <<
                "This shell command downloads the directory recursively into a new directory "
                "(with the same name) in CWD, traversing the directory and its subdirectories recursively."
            >>,
            <<"command">> => [<<"xrdcp">>, <<"-r">>, ?XROOTD_URI(Domain, <<"/", FullDataPath/binary>>), <<".">>]
        },
        #{
            <<"name">> => <<"List directory files and subdirectories">>,
            <<"description">> => <<
                "This shell command returns the list of files "
                "and subdirectories that reside in this directory."
            >>,
            <<"command">> => [<<"xrdfs">>, ?XROOTD_URI(Domain, <<"">>), <<"ls">>, FullDataPath]
        }
    ];
xrootd_api(_, Domain, SpaceId, ShareId, FilePath) ->
    FullDataPath = str_utils:format_bin("/data/~ts/~ts/~ts~ts", [SpaceId, SpaceId, ShareId, FilePath]),
    [
        #{
            <<"name">> => <<"Download file">>,
            <<"description">> => <<
                "This shell command downloads the file content into a new file (with the same name) in CWD."
            >>,
            <<"command">> => [<<"xrdcp">>, ?XROOTD_URI(Domain, <<"/", FullDataPath/binary>>), <<".">>]
        }
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% These samples point to a centralized endpoint in Onezone used to fetch info and contents
%% of any file/directory being a part of any share ("/shares/data/{file_objectid}/...").
%% It redirects to a REST endpoint in one of the supporting providers.
%% @end
%%--------------------------------------------------------------------
-spec rest_api(onedata_file:type(), file_id:objectid()) -> rest_api_samples:record().
rest_api(FileType, FileId) ->
    #rest_api_samples{
        api_root = oneprovider:get_oz_url(oz_plugin:get_oz_rest_api_prefix()),
        samples = rest_api_endpoints(FileType, FileId)
    }.


%% @private
-spec rest_api_endpoints(onedata_file:type(), file_id:objectid()) -> [rest_api_request_sample:record()].
rest_api_endpoints(?DIRECTORY_TYPE, FileId) -> [
    download_directory_endpoint(FileId),
    list_directory_endpoint(FileId),
    get_attributes_endpoint(FileId),
    get_extended_attributes_endpoint(FileId),
    get_json_metadata_endpoint(FileId),
    get_rdf_metadata_endpoint(FileId)
];
rest_api_endpoints(_, FileId) -> [
    download_file_content_endpoint(FileId),
    get_attributes_endpoint(FileId),
    get_extended_attributes_endpoint(FileId),
    get_json_metadata_endpoint(FileId),
    get_rdf_metadata_endpoint(FileId)
].


%% @private
-spec download_directory_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
download_directory_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Download directory (tar)">>,
        description = <<"Returns a TAR archive (binary stream) with directory contents.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/content", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.


%% @private
-spec download_file_content_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
download_file_content_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Download file content">>,
        description = <<"Returns the binary file content.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/content", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.


%% @private
-spec list_directory_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
list_directory_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"List directory files and subdirectories">>,
        description = <<"Returns the list of files and subdirectories that reside in this directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/children", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.


%% @private
-spec get_attributes_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_attributes_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get attributes">>,
        description = <<"Returns basic attributes of the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.


%% @private
-spec get_extended_attributes_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_extended_attributes_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get extended attributes (xattrs)">>,
        description = <<"Returns custom extended attributes (xattrs) associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/metadata/xattrs", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.


%% @private
-spec get_json_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_json_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get JSON metadata">>,
        description = <<"Returns custom JSON metadata associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/metadata/json", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.



%% @private
-spec get_rdf_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_rdf_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get RDF metadata">>,
        description = <<"Returns custom RDF metadata associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/shares/data/~ts/metadata/rdf", [FileId]),
        requires_authorization = false,
        follow_redirects = true,
        swagger_operation_id = <<"get_shared_data">>
    }.
