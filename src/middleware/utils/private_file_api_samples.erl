%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API samples for basic operation on files.
%%% API samples are generated for each file separately and they include
%%% information specific for the file, e.g. its Id. The list of returned
%%% operations depends on the file type.
%%%
%%% NOTE: API samples do not cover all available API, only the commonly used
%%% endpoints and options.
%%% @end
%%%-------------------------------------------------------------------
-module(private_file_api_samples).
-author("Lukasz Opiola").

-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/api_samples/common.hrl").


-export([generate_for/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec generate_for(session:id(), fslogic_worker:file_guid()) -> json_utils:json_term().
generate_for(SessionId, FileGuid) ->
    {ok, #file_attr{type = FileType}} = ?lfm_check(lfm:stat(SessionId, ?FILE_REF(FileGuid))),
    {ok, FileId} = file_id:guid_to_objectid(FileGuid),
    #{
        <<"rest">> => jsonable_record:to_json(gen_samples(FileType, FileId), rest_api_samples)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gen_samples(file_meta:type(), file_id:objectid()) -> rest_api_samples:record().
gen_samples(FileType, FileId) ->
    #rest_api_samples{
        api_root = oneprovider:build_rest_url(<<"">>),
        samples = rest_api_endpoints(FileType, FileId)
    }.


%% @private
-spec rest_api_endpoints(file_meta:type() | file_and_dir, file_id:objectid()) ->
    [rest_api_request_sample:record()].
rest_api_endpoints(?DIRECTORY_TYPE, FileId) ->
    [
        download_directory_endpoint(FileId),
        list_children_endpoint(FileId),
        create_file_endpoint(FileId)
    ] ++ rest_api_endpoints(file_and_dir, FileId);
rest_api_endpoints(?REGULAR_FILE_TYPE, FileId) ->
    [
        download_file_content_endpoint(FileId),
        update_file_content_endpoint(FileId),
        get_hardlinks_endpoint(FileId)
    ] ++ rest_api_endpoints(file_and_dir, FileId);
rest_api_endpoints(file_and_dir, FileId) ->
    [
        get_data_distribution_endpoint(FileId),
        remove_file_endpoint(FileId),
        get_attrs_endpoint(FileId),
        get_json_metadata_endpoint(FileId),
        set_json_metadata_endpoint(FileId),
        remove_json_metadata_endpoint(FileId),
        get_rdf_metadata_endpoint(FileId),
        set_rdf_metadata_endpoint(FileId),
        remove_rdf_metadata_endpoint(FileId),
        get_xattrs_metadata_endpoint(FileId),
        set_xattr_metadata_endpoint(FileId),
        remove_xattrs_metadata_endpoint(FileId)
    ];
rest_api_endpoints(?SYMLINK_TYPE, FileId) ->
    [
        get_symlink_value_endpoint(FileId),
        get_hardlinks_endpoint(FileId),
        get_data_distribution_endpoint(FileId),
        remove_file_endpoint(FileId),
        get_attrs_endpoint(FileId)
    ].


%% @private
-spec create_file_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
create_file_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Create file in directory">>,
        description = <<"Creates a file in the directory.">>,
        method = 'POST',
        path = str_utils:format_bin("/data/~s/children?name=$NAME", [FileId]),
        placeholders = #{
            <<"$NAME">> => <<"Name of the file.">>
        },
        optional_parameters = [
            <<"type">>, <<"mode">>, <<"offset">>, <<"target_file_id">>, <<"target_file_path">>
        ],
        swagger_operation_id = <<"create_file">>
    }.


%% @private
-spec list_children_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
list_children_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"List directory files and subdirectories">>,
        description = <<"Returns the list of files and subdirectories that reside in this directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/children", [FileId]),
        optional_parameters = [
            <<"limit">>, <<"token">>, <<"attribute">>, <<"index">>, <<"tune_for_large_continuous_listing">>
        ],
        swagger_operation_id = <<"list_children">>
    }.


%% @private
-spec download_directory_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
download_directory_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Download directory (tar)">>,
        description = <<"Returns a TAR archive (binary stream) with directory contents.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/content", [FileId]),
        swagger_operation_id = <<"download_file_content">>,
        follow_redirects = true
    }.


%% @private
-spec download_file_content_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
download_file_content_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Download file content">>,
        description = <<"Returns the binary file content.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/content", [FileId]),
        swagger_operation_id = <<"download_file_content">>
    }.


%% @private
-spec update_file_content_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
update_file_content_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Update file content">>,
        description = <<"Overwrites the content of the file.">>,
        method = 'PUT',
        path = str_utils:format_bin("/data/~s/content", [FileId]),
        data = <<"$NEW_CONTENT">>,
        placeholders = #{
            <<"$NEW_CONTENT">> => <<"Binary content that will be written to the file.">>
        },
        optional_parameters = [<<"offset">>],
        swagger_operation_id = <<"update_file_content">>
    }.


%% @private
-spec get_data_distribution_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_data_distribution_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get data distribution">>,
        description = <<"Returns information about data distribution of the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/distribution", [FileId]),
        swagger_operation_id = <<"get_file_distribution">>  % @TODO VFS-10232 rename to get_data_distribution
    }.


%% @private
-spec remove_file_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
remove_file_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Remove file">>,
        description = <<"Removes the file or directory.">>,
        method = 'DELETE',
        path = str_utils:format_bin("/data/~s", [FileId]),
        swagger_operation_id = <<"remove_file">>
    }.


%% @private
-spec get_attrs_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_attrs_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get attributes">>,
        description = <<"Returns basic attributes of the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s", [FileId]),
        optional_parameters = [<<"attribute">>],
        swagger_operation_id = <<"get_attrs">>
    }.


%% @private
-spec get_hardlinks_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_hardlinks_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get file hard links">>,
        description = <<"Returns the Ids of all hard links (including this one) associated with the file.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/hardlinks", [FileId]),
        swagger_operation_id = <<"get_file_hardlinks">>
    }.


%% @private
-spec get_symlink_value_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_symlink_value_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get symbolic link value">>,
        description = <<"Returns the value of the symbolic link.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/symlink_value", [FileId]),
        swagger_operation_id = <<"get_symlink_value">>
    }.


%% @private
-spec get_json_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_json_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get JSON metadata">>,
        description = <<"Returns custom JSON metadata associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/metadata/json", [FileId]),
        optional_parameters = [
            <<"filter_type">>, <<"filter">>, <<"inherited">>, <<"resolve_symlink">>
        ],
        swagger_operation_id = <<"get_json_metadata">>
    }.


%% @private
-spec set_json_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
set_json_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Set JSON metadata">>,
        description = <<"Sets json metadata for the file or directory.">>,
        method = 'PUT',
        path = str_utils:format_bin("/data/~s/metadata/json", [FileId]),
        data = <<"$METADATA">>,
        headers = #{
            <<"content-type">> => <<"application/json">>
        },
        placeholders = #{
            <<"$METADATA">> => <<"The JSON metadata.">>
        },
        optional_parameters = [
            <<"filter_type">>, <<"filter">>, <<"resolve_symlink">>
        ],
        swagger_operation_id = <<"set_json_metadata">>
    }.


%% @private
-spec remove_json_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
remove_json_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Remove JSON metadata">>,
        description = <<"Removes json metadata from the file or directory.">>,
        method = 'DELETE',
        path = str_utils:format_bin("/data/~s/metadata/json", [FileId]),
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"remove_json_metadata">>
    }.


%% @private
-spec get_rdf_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_rdf_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get RDF metadata">>,
        description = <<"Returns custom RDF metadata associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/metadata/rdf", [FileId]),
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"get_rdf_metadata">>
    }.


%% @private
-spec set_rdf_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
set_rdf_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Set RDF metadata">>,
        description = <<"Sets rdf metadata for the file or directory.">>,
        method = 'PUT',
        path = str_utils:format_bin("/data/~s/metadata/rdf", [FileId]),
        data = <<"$METADATA">>,
        headers = #{
            <<"content-type">> => <<"application/rdf+xml">>
        },
        placeholders = #{
            <<"$METADATA">> => <<"The rdf metadata (XML).">>
        },
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"set_rdf_metadata">>
    }.


%% @private
-spec remove_rdf_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
remove_rdf_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Remove RDF metadata">>,
        description = <<"Removes rdf metadata from the file or directory.">>,
        method = 'DELETE',
        path = str_utils:format_bin("/data/~s/metadata/rdf", [FileId]),
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"remove_rdf_metadata">>
    }.


%% @private
-spec get_xattrs_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
get_xattrs_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Get extended attributes (xattrs)">>,
        description = <<"Returns custom extended attributes (xattrs) associated with the file or directory.">>,
        method = 'GET',
        path = str_utils:format_bin("/data/~s/metadata/xattrs", [FileId]),
        optional_parameters = [
            <<"attribute">>, <<"inherited">>, <<"resolve_symlink">>
        ],
        swagger_operation_id = <<"get_xattrs">>
    }.


%% @private
-spec set_xattr_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
set_xattr_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Set extended attribute (xattr)">>,
        description = <<"Sets extended attribute (xattr) for the file or directory.">>,
        method = 'PUT',
        path = str_utils:format_bin("/data/~s/metadata/xattrs", [FileId]),
        data = <<"$XATTRS">>,
        headers = #{
            <<"content-type">> => <<"application/json">>
        },
        placeholders = #{
            <<"$XATTRS">> => <<"Key-value pairs to be set as extended attributes (a JSON object).">>
        },
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"set_xattr">>
    }.


%% @private
-spec remove_xattrs_metadata_endpoint(file_id:objectid()) -> rest_api_request_sample:record().
remove_xattrs_metadata_endpoint(FileId) ->
    #rest_api_request_sample{
        name = <<"Remove extended attributes (xattrs)">>,
        description = <<"Removes extended attributes (xattrs) from the file or directory.">>,
        method = 'DELETE',
        path = str_utils:format_bin("/data/~s/metadata/xattrs", [FileId]),
        data = <<"{\"keys\": $KEY_LIST}">>,
        headers = #{
            <<"content-type">> => <<"application/json">>
        },
        placeholders = #{
            <<"$KEY_LIST">> => <<"The extended attribute keys to remove (a JSON array of strings).">>
        },
        optional_parameters = [<<"resolve_symlink">>],
        swagger_operation_id = <<"remove_xattrs">>
    }.
