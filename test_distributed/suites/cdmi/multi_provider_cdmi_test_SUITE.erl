%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_cdmi_test_SUITE).
-author("Tomasz Lichon").

-include_lib("ctool/include/http/headers.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_dir_test/1,
    get_file_test/1,
    metadata_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    create_file_test/1,
    update_file_test/1,
    create_dir_test/1,
    capabilities_test/1,
    use_supported_cdmi_version_test/1,
    use_unsupported_cdmi_version_test/1,
    moved_permanently_test/1,
    objectid_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_test/1,
    out_of_range_test/1,
    partial_upload_test/1,
    acl_test/1,
    errors_test/1,
    accept_header_test/1,
    move_copy_conflict_test/1,
    move_test/1,
    copy_test/1
]).

all() -> [
        delete_dir_test,
        list_dir_test,
        get_file_test,
        metadata_test,
        delete_file_test,
        create_file_test,
        update_file_test,
        create_dir_test,
        capabilities_test,
        use_supported_cdmi_version_test,
        use_unsupported_cdmi_version_test,
        moved_permanently_test,
        objectid_test,
        request_format_check_test,
        mimetype_and_encoding_test,
        out_of_range_test,
        partial_upload_test,
        acl_test,
        errors_test,
        accept_header_test,
        move_copy_conflict_test,
        move_test,
        copy_test
].

-define(TIMEOUT, timer:seconds(5)).

-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>}).

-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).

%%%===================================================================
%%% Test functions
%%%===================================================================


list_dir_test(Config) ->
    cdmi_test_base:list_dir(Config, krakow, paris, space_krk_par_p).


get_file_test(Config) ->
    cdmi_test_base:get_file(Config, krakow, paris, space_krk_par_p).


metadata_test(Config) ->
    cdmi_test_base:metadata(Config, krakow, paris, space_krk_par_p).


delete_file_test(Config) ->
    cdmi_test_base:delete_file(Config, krakow, paris, space_krk_par_p).


delete_dir_test(Config) ->
    cdmi_test_base:delete_dir(Config, krakow, paris, space_krk_par_p).


create_file_test(Config) ->
    cdmi_test_base:create_file(Config, krakow, paris, space_krk_par_p).


update_file_test(Config) ->
    cdmi_test_base:update_file(Config, krakow, paris, space_krk_par_p).


create_dir_test(Config) ->
    cdmi_test_base:create_dir(Config, krakow, paris, space_krk_par_p).


objectid_test(Config) ->
    cdmi_test_base:objectid(Config, krakow, paris, space_krk_par_p).


capabilities_test(Config) ->
    cdmi_test_base:capabilities(Config, krakow, paris).


use_supported_cdmi_version_test(Config) ->
    cdmi_test_base:use_supported_cdmi_version(Config, krakow, paris).


use_unsupported_cdmi_version_test(Config) ->
    cdmi_test_base:use_unsupported_cdmi_version(Config, krakow, paris).


moved_permanently_test(Config) ->
    cdmi_test_base:moved_permanently(Config, krakow, paris, space_krk_par_p).


request_format_check_test(Config) ->
    cdmi_test_base:request_format_check(Config, krakow, paris, space_krk_par_p).


mimetype_and_encoding_test(Config) ->
    cdmi_test_base:mimetype_and_encoding(Config, krakow, paris, space_krk_par_p).


out_of_range_test(Config) ->
    cdmi_test_base:out_of_range(Config, krakow, paris, space_krk_par_p).


move_copy_conflict_test(Config) ->
    cdmi_test_base:move_copy_conflict(Config, krakow, paris, space_krk_par_p).


move_test(Config) ->
    cdmi_test_base:move(Config, krakow, paris, space_krk_par_p).


copy_test(Config) ->
    cdmi_test_base:copy(Config, krakow, paris, space_krk_par_p).


partial_upload_test(Config) ->
    cdmi_test_base:partial_upload(Config, krakow, paris, space_krk_par_p).


acl_test(Config) ->
    cdmi_test_base:acl(Config, krakow, paris, space_krk_par_p).


errors_test(Config) ->
    cdmi_test_base:errors(Config, krakow, paris, space_krk_par_p).


accept_header_test(Config) ->
    cdmi_test_base:accept_header(Config, krakow, paris).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config),
    Config.


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    ok.

