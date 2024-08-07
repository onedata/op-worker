%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Utility functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(space_setup_utils).
-author("Katarzyna Such").

-include("modules/fslogic/fslogic_common.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-type s3_storage_params() :: #s3_storage_params{}.
-type posix_storage_params() :: #posix_storage_params{}.
-type support_spec() :: #support_spec{}.
-type space_spec() :: #space_spec{}.

-export_type([posix_storage_params/0, s3_storage_params/0, support_spec/0]).

-define(CURRENT_DATETIME(), time:seconds_to_datetime(global_clock:timestamp_seconds())).

%% API
-export([create_storage/2, set_up_space/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create_storage(oct_background:node_selector(), s3_storage_params() | posix_storage_params())
        -> od_storage:id().
create_storage(Provider, #s3_storage_params{storage_path_type = StoragePathType,
    imported_storage = Imported, hostname = Hostname, bucket_name = BucketName,
    access_key = AccessKey, secret_key = SecretKey, block_size = BlockSize
} = S3StorageParam) ->
    create_bucket(Provider, S3StorageParam),
    CreateStorageData = #{?RAND_STR() => #{
        <<"type">> => <<"s3">>,
        <<"storagePathType">> => StoragePathType,
        <<"importedStorage">> => Imported,
        <<"hostname">> => Hostname,
        <<"bucketName">> => BucketName,
        <<"accessKey">> => AccessKey,
        <<"secretKey">> => SecretKey,
        <<"blockSize">> => BlockSize
    }},
    panel_test_rpc:add_storage(Provider, CreateStorageData);
create_storage(Provider, #posix_storage_params{mount_point = MountPoint, imported_storage = Imported}) ->
    ?assertMatch(ok, opw_test_rpc:call(Provider, filelib, ensure_path, [MountPoint])),
    panel_test_rpc:add_storage(Provider,
        #{?RAND_STR() => #{
            <<"type">> => <<"posix">>,
            <<"mountPoint">> => MountPoint,
            <<"importedStorage">> => Imported
        }}
    ).


-spec set_up_space(space_spec()) -> oct_background:entity_id().
set_up_space(SpaceSpec = #space_spec{
    name = SpaceName,
    owner = OwnerSelector,
    users = Users,
    supports = SupportSpecs
}) ->
    NameBinary = case SpaceName of
        undefined -> str_utils:rand_hex(8);
        Binary when is_binary(Binary) -> Binary;
        Atom when is_atom(Atom) -> atom_to_binary(Atom)
    end,
    OwnerId = oct_background:get_user_id(OwnerSelector),
    SpaceId = ozw_test_rpc:create_space(OwnerId, NameBinary),

    SupportToken = ozw_test_rpc:create_space_support_token(OwnerId, SpaceId),
    support_space(SupportSpecs, SupportToken),

    add_users_to_space(Users, SpaceId),
    force_fetch_entities(SpaceId, SpaceSpec),
    SpaceId.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec support_space([support_spec()], tokens:serialized()) -> ok.
support_space(SupportSpecs, SupportToken) ->
    lists:foreach(fun(#support_spec{provider = Provider, storage_spec = StorageSpec, size = Size}) ->
        StorageId = case StorageSpec of
            any -> lists_utils:random_element(opw_test_rpc:get_storages(Provider));
            Id when is_binary(Id) -> Id;
            Spec when is_tuple(Spec) -> create_storage(Provider, Spec)
        end,
        panel_test_rpc:support_space(Provider, StorageId, SupportToken, Size)
    end, SupportSpecs).


%% @private
-spec add_users_to_space([oct_background:entity_selector()], oct_background:entity_id()) -> ok.
add_users_to_space(Users, SpaceId) ->
    lists:foreach(fun(User) ->
        UserId = oct_background:get_user_id(User),
        ozw_test_rpc:add_user_to_space(SpaceId, UserId)
    end, Users).


%% @private
-spec force_fetch_entities(od_space:id(), space_spec()) -> ok.
force_fetch_entities(SpaceId, #space_spec{
    owner = OwnerSelector,
    users = Users,
    supports = Supports
}) ->
    ProviderSelectors = lists:map(fun(SupportSpec) -> SupportSpec#support_spec.provider end, Supports),
    opt:force_fetch_entity(od_space, SpaceId, ProviderSelectors),
    lists:foreach(fun(User) ->
        UserId = oct_background:get_user_id(User),
        opt:force_fetch_entity(od_user, UserId, ProviderSelectors)
    end, [OwnerSelector | Users]).


%% @private
-spec create_bucket(oct_background:node_selector(), s3_storage_params()) -> ok.
create_bucket(Provider, #s3_storage_params{bucket_name = BucketName,
    access_key = AccessKey, secret_key = SecretKey
}) ->
    Hostname = build_s3_hostname(Provider),
    Url = str_utils:format("http://~ts/~ts", [Hostname, BucketName]),

    DateTime = datetime_to_compact_iso8601(?CURRENT_DATETIME()),
    AmzContent = hash_to_hex_list(crypto:hash(sha256, "")),
    Authorization = s3_authorization(Hostname, AmzContent, DateTime, BucketName, SecretKey, AccessKey),

    Headers = #{
        <<"Content-Length">> => 0,
        <<"X-Amz-Date">> => DateTime,
        <<"X-Amz-Content-SHA256">> => AmzContent,
        <<"Authorization">> => Authorization
    },
    {ok, 200, _, _} = opw_test_rpc:call(Provider, http_client, put, [Url, Headers, "", []]),
    ok.



%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Creates an s3 authorization key according to the documentation:
%% https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
%% @end
%%------------------------------------------------------------------------------
-spec s3_authorization(string(), string(), string(), binary(), binary(), binary()) -> string().
s3_authorization(Hostname, AmzContent, DateTime, BucketName, SecretKey, AccessKey) ->
    CanonicalHeaders = str_utils:format(
        "host:~ts\n"
        "x-amz-content-sha256:~ts\n"
        "x-amz-date:~ts",
        [Hostname, AmzContent, DateTime]
    ),
    SignedHeaders = "host;x-amz-content-sha256;x-amz-date",
    CanonicalRequest = str_utils:format(
        "PUT\n/~ts\n\n"
        "~ts\n\n"
        "~ts\n"
        "~ts",
        [BucketName, CanonicalHeaders, SignedHeaders, AmzContent]
    ),

    Date = lists:sublist(DateTime, 8),
    CredentialScope = str_utils:format("~ts/eu-central-1/s3/aws4_request", [Date]),
    HashedCanonicalRequest = hash_to_hex_list(crypto:hash(sha256, CanonicalRequest)),

    StringToSign = str_utils:format(
        "AWS4-HMAC-SHA256\n~ts\n"
        "~ts\n"
        "~ts",
        [DateTime, CredentialScope, HashedCanonicalRequest]
    ),
    SigningKey = s3_signing_key(SecretKey, Date),
    Signature = hash_to_hex_list(crypto:mac(hmac, sha256, SigningKey, StringToSign)),

    str_utils:format(
        "AWS4-HMAC-SHA256 Credential=~ts/~ts, SignedHeaders=~ts, Signature=~ts",
        [AccessKey, CredentialScope, SignedHeaders, Signature]
    ).


%% @private
-spec build_s3_hostname(oct_background:entity_selector()) -> string().
build_s3_hostname(ProviderSelector) ->
    %% hostname structure results from onenv charts
    "volume-s3.dev-volume-s3-" ++ atom_to_list(oct_background:to_entity_placeholder(ProviderSelector)) ++ ":9000".


%% @private
-spec s3_signing_key(string(), string()) -> binary().
s3_signing_key(SecretKey, Date) ->
    KDate = crypto:mac(hmac, sha256, "AWS4" ++ SecretKey, Date),
    KRegion = crypto:mac(hmac, sha256, KDate, "eu-central-1"),
    KService = crypto:mac(hmac, sha256, KRegion, "s3"),
    crypto:mac(hmac, sha256, KService, "aws4_request").


%% @private
-spec hash_to_hex_list(binary()) -> string().
hash_to_hex_list(Hash) ->
    binary_to_list(hex_utils:hex(Hash)).


%% @private
%% @doc AWS CLI requires X-Amz-Date to be in yyyyMMddTHHmmssZ ISO 8601 format
-spec datetime_to_compact_iso8601(calendar:datetime()) -> string().
datetime_to_compact_iso8601({{Y,Mo,D}, {H,Mn,S}}) when is_float(S) ->
    io_lib:format("~4.10.0B~2.10.0B~2.10.0BT~2.10.0B~2.10.0B~9.6.0fZ", [Y, Mo, D, H, Mn, S]);
datetime_to_compact_iso8601({{Y,Mo,D}, {H,Mn,S}}) ->
    io_lib:format("~4.10.0B~2.10.0B~2.10.0BT~2.10.0B~2.10.0B~2.10.0BZ", [Y, Mo, D, H, Mn, S]).

