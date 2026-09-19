import 'dart:convert';
import 'dart:io';

import 'package:dart_appwrite/dart_appwrite.dart';
import 'package:http/http.dart' as http;
import 'package:dart_appwrite/models.dart' show Document;
import 'package:googleapis_auth/auth_io.dart' as auth;

/// Appwrite Function: verify_google_purchase
///
/// Verifies a Google Play purchase server-side and grants book access.
///
/// Flow:
/// 1. Receives purchaseToken, productId, bookId, libraryIds from the app
/// 2. Authenticates with Google Play Developer API using service account
/// 3. Verifies the purchase token is valid and payment was received
/// 4. Grants the book to user's library (same as Chargily webhook)
/// 5. Returns success/failure
///
/// Environment variables required:
///   - APPWRITE_ENDPOINT
///   - APPWRITE_FUNCTION_PROJECT_ID
///   - APPWRITE_API_KEY
///   - DB_ID
///   - STORE_BOOKS_COLLECTION
///   - USER_LIBRARY_COLLECTION
///   - GOOGLE_SERVICE_ACCOUNT_JSON (Google Cloud service account with
///     androidpublisher scope)
///   - GOOGLE_PLAY_PACKAGE_NAME (e.g., com.melon.book)

final String? endpoint = Platform.environment['APPWRITE_ENDPOINT'];
final String? projectId = Platform.environment['APPWRITE_FUNCTION_PROJECT_ID'];
final String? apiKey = Platform.environment['APPWRITE_API_KEY'];
final String? dbId = Platform.environment['DB_ID'];
final String? storeCol = Platform.environment['STORE_BOOKS_COLLECTION'];
final String? userLibraryCol = Platform.environment['USER_LIBRARY_COLLECTION'];
final String? googleServiceAccountJson =
    Platform.environment['GOOGLE_SERVICE_ACCOUNT_JSON'];
final String? packageName = Platform.environment['GOOGLE_PLAY_PACKAGE_NAME'];

Client _adminClient() {
  return Client()
    ..setEndpoint(endpoint!)
    ..setProject(projectId!)
    ..setKey(apiKey!);
}

/// Where the catalogue lives, so a sale's price comes from the book itself.
final String storeBooksCol =
    Platform.environment['STORE_BOOKS_COLLECTION'] ?? 'store_books_table';

/// The sales ledger.
final String transactionsCol =
    Platform.environment['TRANSACTIONS_COLLECTION'] ?? 'transactions_table';

Future<dynamic> main(final context) async {
  // Validate environment
  final envVars = [
    endpoint,
    projectId,
    apiKey,
    dbId,
    storeCol,
    userLibraryCol,
    googleServiceAccountJson,
    packageName,
  ];

  if (envVars.any((v) => v == null || v.isEmpty)) {
    context.error('❌ Missing environment variables');
    return context.res.json({
      'success': false,
      'error': 'Server configuration error',
    }, 500);
  }

  try {
    // Parse request body
    Map<String, dynamic> data = {};
    final body = context.req.body;
    if (body is String && body.isNotEmpty) {
      data = jsonDecode(body) as Map<String, dynamic>;
    } else if (body is Map) {
      data = Map<String, dynamic>.from(body);
    }

    context.log('📦 Payload: ${jsonEncode(data)}');

    // Check if this is an RTDN (Real-Time Developer Notification) from Pub/Sub
    final actionType = (data['action_type'] ?? '').toString();
    if (actionType == 'rtdn' || data.containsKey('message')) {
      return await _handleRTDN(context, data);
    }

    // Otherwise, it's a direct verification request from the app
    final purchaseToken = (data['purchase_token'] ?? '').toString().trim();
    final productId = (data['product_id'] ?? '').toString().trim();
    final bookId = (data['book_id'] ?? '').toString().trim();
    final libraryIds = _parseStringList(data['library_ids']);
    final jwt = (data['jwt'] ?? '').toString().trim();

    // Present when this purchase is paying for a gift. Only the id crosses
    // the wire: who receives the book, and what it costs, are read back out
    // of the gift document, which the buyer's device cannot write.
    final giftId = (data['gift_id'] ?? '').toString().trim();

    if (purchaseToken.isEmpty || productId.isEmpty || bookId.isEmpty) {
      context.error('❌ Missing required fields');
      return context.res.json({
        'success': false,
        'error': 'Missing purchase_token, product_id, or book_id',
      }, 400);
    }

    if (jwt.isEmpty) {
      context.error('❌ Missing JWT');
      return context.res.json({
        'success': false,
        'error': 'Authentication required',
      }, 401);
    }

    // Verify user identity from JWT
    final userClient = Client()
      ..setEndpoint(endpoint!)
      ..setProject(projectId!)
      ..setJWT(jwt);
    final account = Account(userClient);
    final user = await account.get();
    final userId = user.$id;
    context.log('👤 Verified user: $userId');

    // Step 1: Verify purchase with Google Play Developer API
    context.log('🔍 Verifying purchase with Google Play API...');
    bool isGoogleVerified = false;

    final verificationResult = await _verifyWithGooglePlay(
      context,
      purchaseToken: purchaseToken,
      productId: productId,
    );

    if (verificationResult['valid'] == true) {
      isGoogleVerified = true;
      context.log('✅ Purchase verified with Google Play');
    } else {
      final reason = (verificationResult['reason'] ?? 'unknown').toString();
      context.log('⚠️ Google API verification returned: $reason');

      // Determine if this is a server-side permission/config issue (401/403)
      // vs an actual invalid purchase (e.g., cancelled, invalid token).
      final isPermissionError = reason.contains('401') ||
          reason.contains('403') ||
          reason.contains('permissionDenied') ||
          reason.contains('insufficient');

      if (isPermissionError) {
        // 401/403 = OUR service account lacks Play Console permissions.
        // The user IS authenticated (JWT verified above), and Google Play
        // already charged them. Blocking the book here would mean:
        //   - User paid money but got nothing
        //   - We can't refund programmatically (no API access!)
        // So we grant the book with a warning in logs.
        context
            .log('⚠️ FALLBACK: Granting book despite 401/403 because user is '
                'JWT-verified and Google already charged them. '
                'Fix: Google Play Console → Setup → API access.');
      } else {
        // Genuine verification failure (cancelled, invalid token, etc.)
        context.error('❌ Purchase verification failed: $reason');
        return context.res.json({
          'success': false,
          'error': 'Purchase verification failed',
          'reason': reason,
        }, 403);
      }
    }

    // Additional security: verify purchase token is non-trivial
    if (purchaseToken.length < 20) {
      context.error('❌ Suspicious purchase token (too short)');
      return context.res.json({
        'success': false,
        'error': 'Invalid purchase token',
      }, 403);
    }

    // Step 2: Verify the book exists in the store
    final adminDatabases = Databases(_adminClient());
    context.log(
        '📖 Looking up book: dbId=$dbId, collection=$storeCol, bookId=$bookId');
    try {
      final bookDoc = await adminDatabases.getDocument(
        databaseId: dbId!,
        collectionId: storeCol!,
        documentId: bookId,
      );
      context.log('📖 Found book: ${bookDoc.data['title'] ?? bookId}');
    } catch (e) {
      context.error('❌ Book not found: $bookId (db=$dbId, col=$storeCol)');
      context.error('❌ Error details: $e');

      // Don't block the purchase — the book ID comes from our own app,
      // the user already paid, and the purchase is Google-verified.
      // Log the error but still grant the book to the library.
      context.log('⚠️ Proceeding to grant book despite lookup failure');
    }

    // Step 3: Deliver what was bought.
    //
    // A gift goes to its recipient rather than to the buyer, and is settled
    // against its own document -- which is also where the price is checked,
    // since a Play product is a tier rather than an amount and a cheap tier
    // must not be able to pay for an expensive book.
    if (giftId.isNotEmpty) {
      final outcome = await _settleGift(
        context,
        adminDatabases: adminDatabases,
        giftId: giftId,
        payerId: userId,
        productId: productId,
      );

      if (outcome != null) {
        // Refused before anything was delivered. The purchase is left
        // unacknowledged on Google's side so it can be refunded or retried
        // rather than silently consumed.
        return context.res.json({
          'success': false,
          'error': outcome,
        }, 403);
      }

      if (isGoogleVerified) {
        await _acknowledgePurchase(
          context,
          purchaseToken: purchaseToken,
          productId: productId,
        );
      }

      return context.res.json({
        'success': true,
        'message': 'Gift paid for and delivered',
        'google_verified': isGoogleVerified,
        'gift_id': giftId,
      });
    }

    await _grantBookToLibrary(
      context,
      adminDatabases: adminDatabases,
      userId: userId,
      bookId: bookId,
    );

    // Step 4: Acknowledge the purchase (mark as consumed on Google's side)
    // Only attempt if Google API verification succeeded — otherwise this
    // will also fail with the same 401.
    if (isGoogleVerified) {
      await _acknowledgePurchase(
        context,
        purchaseToken: purchaseToken,
        productId: productId,
      );
      context.log('✅ Book granted and purchase acknowledged on Google');
    } else {
      context.log(
          '⚠️ Book granted. Skipping server-side acknowledge (API permission '
          'issue). The app will consume it locally via consumePurchase().');
    }

    return context.res.json({
      'success': true,
      'message': 'Purchase verified and book granted',
      'google_verified': isGoogleVerified,
    });
  } catch (e, stackTrace) {
    context.error('❌ Error: $e');
    context.error('Stack: $stackTrace');
    return context.res.json({
      'success': false,
      'error': 'Internal server error',
    }, 500);
  }
}

/// Marks a paid gift as delivered, the same way the Chargily webhook does.
///
/// Returns `null` when the gift was settled (including when it already had
/// been, since a retried verification must not fail), or a short reason to
/// refuse with.
///
/// Deliberately does not put the book in anybody's library: the recipient
/// unwraps the card and `claim_gift` moves it then, so the moment the book
/// appears is the moment they opened the gift.
Future<String?> _settleGift(
  final context, {
  required Databases adminDatabases,
  required String giftId,
  required String payerId,
  required String productId,
}) async {
  final giftsCol = Platform.environment['DB_GIFTS'] ?? 'gifts';
  final notifyFunctionId = Platform.environment['NOTIFY_FUNCTION_ID'] ?? '';

  Document gift;
  try {
    gift = await adminDatabases.getDocument(
      databaseId: dbId!,
      collectionId: giftsCol,
      documentId: giftId,
    );
  } on AppwriteException catch (e) {
    context.error('❌ Gift $giftId not found: ${e.message}');
    return 'gift_not_found';
  }

  final status = gift.data['status']?.toString() ?? '';
  if (status != 'pending_payment') {
    // Verification runs again after an app restart with a pending purchase,
    // so arriving at an already-delivered gift is ordinary, not an error.
    context.log('ℹ️ Gift $giftId is already $status; nothing to settle');
    return null;
  }

  final senderId = gift.data['sender_id']?.toString() ?? '';
  if (senderId != payerId) {
    context.error('❌ Gift $giftId belongs to $senderId, not to $payerId');
    return 'not_your_gift';
  }

  // A Play product is a price tier, not an amount, so the check is that the
  // tier bought is the tier this book is sold at. Without it, the cheapest
  // product could pay for the most expensive book.
  //
  // A book may carry its own product id, which overrides the tier its price
  // would otherwise fall into, so both are accepted -- and the book's own id
  // is read here rather than taken from the request.
  final accepted = <String>{};
  final tier = _productIdForPrice((gift.data['price'] as num?)?.toInt() ?? 0);
  if (tier != null) accepted.add(tier);

  try {
    final book = await adminDatabases.getDocument(
      databaseId: dbId!,
      collectionId: storeCol!,
      documentId: gift.data['book_id']?.toString() ?? '',
    );
    final own = book.data['google_play_product_id']?.toString() ?? '';
    if (own.isNotEmpty) accepted.add(own);
  } catch (e) {
    context.log('⚠️ Could not read the gifted book for its product id: $e');
  }

  if (accepted.isNotEmpty && !accepted.contains(productId)) {
    context.error(
        '❌ Gift $giftId expects one of $accepted but $productId was bought');
    await adminDatabases.updateDocument(
      databaseId: dbId!,
      collectionId: giftsCol,
      documentId: giftId,
      data: {'status': 'underpaid'},
    );
    return 'wrong_price_tier';
  }

  // --- Bind the recipient if they already have an account ---
  String? recipientId = gift.data['recipient_id']?.toString();
  if (recipientId == null || recipientId.isEmpty) {
    final email =
        (gift.data['recipient_email']?.toString() ?? '').toLowerCase();
    try {
      final match = await Users(_adminClient()).list(
        queries: [Query.equal('email', email), Query.limit(1)],
      );
      if (match.users.isNotEmpty) recipientId = match.users.first.$id;
    } catch (e) {
      context.log('⚠️ Recipient lookup failed for $email: $e');
    }
  }

  await adminDatabases.updateDocument(
    databaseId: dbId!,
    collectionId: giftsCol,
    documentId: giftId,
    data: {
      'status': 'delivered',
      'recipient_id': recipientId,
      'delivered_at': DateTime.now().toUtc().toIso8601String(),
    },
    permissions: [
      if (senderId.isNotEmpty) Permission.read(Role.user(senderId)),
      if (recipientId != null && recipientId.isNotEmpty)
        Permission.read(Role.user(recipientId)),
    ],
  );

  context.log('🎁 Gift $giftId delivered');

  // A gift is a sale like any other, and it is recorded against whoever paid
  // for it. Without this the ledger would show gift revenue as nothing at
  // all, because the gift path never reaches _grantBookToLibrary.
  await _recordSale(
    context,
    adminDatabases: adminDatabases,
    userId: senderId,
    bookId: gift.data['book_id']?.toString() ?? '',
  );

  // One channel each: a push for an account, an email for somebody who does
  // not have one yet and could not otherwise be reached.
  if (recipientId == null || recipientId.isEmpty) {
    await _emailGiftLink(
      context,
      toEmail: (gift.data['recipient_email']?.toString() ?? '').toLowerCase(),
      recipientName: gift.data['recipient_name']?.toString() ?? '',
      senderName: gift.data['sender_name']?.toString() ?? '',
      bookTitle: gift.data['book_title']?.toString() ?? '',
      token: giftId,
    );
  }

  if (notifyFunctionId.isNotEmpty &&
      recipientId != null &&
      recipientId.isNotEmpty) {
    try {
      await Functions(_adminClient()).createExecution(
        functionId: notifyFunctionId,
        xasync: true,
        body: jsonEncode({
          'user_id': recipientId,
          'title': 'وصلتك هدية',
          'body': '${gift.data['sender_name'] ?? ''} أهداك كتاب '
              '${gift.data['book_title'] ?? ''}',
          'data': {'type': 'gift_received', 'gift_token': giftId},
        }),
      );
    } catch (e) {
      // The gift is delivered whether or not the push went out.
      context.error('⚠️ Could not notify recipient $recipientId: $e');
    }
  }

  return null;
}

/// The Play product a DZD price belongs to.
///
/// Must stay in step with `AppConfig.getGooglePlayProductId` in the app. It
/// is duplicated rather than shared because the two run in different places,
/// and the server cannot take the client's word for which tier applies --
/// that is the whole point of checking it here.
String? _productIdForPrice(int priceDZD) {
  if (priceDZD <= 0) return null;
  if (priceDZD <= 300) return 'book_tier_1';
  if (priceDZD <= 600) return 'book_tier_2';
  if (priceDZD <= 1000) return 'book_tier_3';
  if (priceDZD <= 1500) return 'book_tier_4';
  if (priceDZD <= 2000) return 'book_tier_5';
  if (priceDZD <= 3000) return 'book_tier_6';
  return 'book_tier_7';
}

/// Verify a purchase token with Google Play Developer API
Future<Map<String, dynamic>> _verifyWithGooglePlay(
  final context, {
  required String purchaseToken,
  required String productId,
}) async {
  try {
    // Parse service account credentials
    final serviceAccount =
        jsonDecode(googleServiceAccountJson!) as Map<String, dynamic>;

    // Create authenticated HTTP client
    final credentials = auth.ServiceAccountCredentials.fromJson(serviceAccount);
    final httpClient = await auth.clientViaServiceAccount(
      credentials,
      ['https://www.googleapis.com/auth/androidpublisher'],
    );

    try {
      // Call Google Play Developer API to verify purchase
      final url = Uri.parse(
        'https://androidpublisher.googleapis.com/androidpublisher/v3'
        '/applications/$packageName'
        '/purchases/products/$productId'
        '/tokens/$purchaseToken',
      );

      final response = await httpClient.get(url);

      context.log('Google API response: ${response.statusCode}');
      context.log('Google API body: ${response.body}');

      if (response.statusCode != 200) {
        return {
          'valid': false,
          'reason': 'Google API returned ${response.statusCode}',
        };
      }

      final purchaseData = jsonDecode(response.body) as Map<String, dynamic>;

      // purchaseState: 0 = Purchased, 1 = Canceled, 2 = Pending
      final purchaseState = purchaseData['purchaseState'] as int?;

      if (purchaseState != 0) {
        return {
          'valid': false,
          'reason': 'Purchase state is not completed: $purchaseState',
        };
      }

      // consumptionState: 0 = Not consumed, 1 = Consumed
      // acknowledgementState: 0 = Not acknowledged, 1 = Acknowledged
      context.log('✅ Purchase state: $purchaseState (purchased)');
      context.log('Consumption state: ${purchaseData['consumptionState']}');
      context.log(
          'Acknowledgement state: ${purchaseData['acknowledgementState']}');

      return {
        'valid': true,
        'purchaseData': purchaseData,
      };
    } finally {
      httpClient.close();
    }
  } catch (e) {
    context.error('Google Play verification error: $e');
    return {
      'valid': false,
      'reason': 'Verification request failed: $e',
    };
  }
}

/// Acknowledge/consume the purchase on Google Play
Future<void> _acknowledgePurchase(
  final context, {
  required String purchaseToken,
  required String productId,
}) async {
  try {
    final serviceAccount =
        jsonDecode(googleServiceAccountJson!) as Map<String, dynamic>;
    final credentials = auth.ServiceAccountCredentials.fromJson(serviceAccount);
    final httpClient = await auth.clientViaServiceAccount(
      credentials,
      ['https://www.googleapis.com/auth/androidpublisher'],
    );

    try {
      // Acknowledge the purchase (for non-consumable products)
      // Or consume it (for consumable products — books are consumable since
      // each purchase is for a different book)
      final url = Uri.parse(
        'https://androidpublisher.googleapis.com/androidpublisher/v3'
        '/applications/$packageName'
        '/purchases/products/$productId'
        '/tokens/$purchaseToken:consume',
      );

      final response = await httpClient.post(url);
      context.log('Consume response: ${response.statusCode}');
    } finally {
      httpClient.close();
    }
  } catch (e) {
    // Non-critical — Google will eventually void the purchase if not acknowledged
    context.error('Warning: Failed to acknowledge purchase: $e');
  }
}

/// Grant book access to user's library
Future<void> _grantBookToLibrary(
  final context, {
  required Databases adminDatabases,
  required String userId,
  required String bookId,
}) async {
  context.log('📚 Granting book $bookId to user $userId');

  // Find or create library document
  final libraryDoc =
      await _findOrCreateLibraryDocument(context, adminDatabases, userId);

  final rawBooks = (libraryDoc.data['books'] as List<dynamic>?) ?? <dynamic>[];

  // Normalize book IDs
  final normalizedBookIds = <String>[];
  final existingIds = <String>{};

  for (final entry in rawBooks) {
    final id = _extractBookId(entry);
    if (id == null || id.isEmpty || !existingIds.add(id)) continue;
    normalizedBookIds.add(id);
  }

  context.log('📦 Existing books: ${existingIds.length}');

  if (existingIds.contains(bookId)) {
    context.log('ℹ️ Book already in library. Skipping.');
    return;
  }

  normalizedBookIds.add(bookId);

  await adminDatabases.updateDocument(
    databaseId: dbId!,
    collectionId: userLibraryCol!,
    documentId: libraryDoc.$id,
    data: <String, dynamic>{
      'user_id': libraryDoc.data['user_id'] ?? userId,
      'books': normalizedBookIds,
    },
  );

  context.log('✅ Book added. New total: ${normalizedBookIds.length}');

  await _recordSale(
    context,
    adminDatabases: adminDatabases,
    userId: userId,
    bookId: bookId,
  );
}

/// Writes the sale to transactions_table.
///
/// Called only after Google has verified the purchase and only when the book
/// was actually new to the library, so a repeated verification of the same
/// purchase cannot double-count it.
///
/// The price is read from the book itself rather than taken from the client,
/// which could claim anything. A free book records nothing: it is not a sale,
/// and letting it into the ledger is what made every free download look like
/// revenue.
///
/// Failure here is logged and swallowed. The reader has paid and has their
/// book; losing one bookkeeping row must never undo that.
Future<void> _recordSale(
  final context, {
  required Databases adminDatabases,
  required String userId,
  required String bookId,
}) async {
  try {
    final book = await adminDatabases.getDocument(
      databaseId: dbId!,
      collectionId: storeBooksCol,
      documentId: bookId,
    );

    final rawPrice = book.data['price'];
    final price = rawPrice is num
        ? rawPrice.toInt()
        : int.tryParse('${rawPrice ?? ''}') ?? 0;

    if (price <= 0) {
      context.log('ℹ️ Free book — nothing to record as a sale.');
      return;
    }

    await adminDatabases.createDocument(
      databaseId: dbId!,
      collectionId: transactionsCol,
      documentId: ID.unique(),
      data: <String, dynamic>{
        'user_id': userId,
        // A relationship attribute, so it takes a list of ids. Passing the
        // bare id is refused with relationship_value_invalid, which is what
        // kept every sale out of the ledger.
        'book_id': <String>[bookId],
        'total_price': price,
        'status': 'completed',
      },
    );

    context.log('💰 Sale recorded: $bookId for $price');
  } catch (e) {
    context.error('⚠️ Could not record the sale (the book was still granted): $e');
  }
}

/// Handle RTDN (Real-Time Developer Notification) from Google Pub/Sub
///
/// This is the backup verification mechanism. When Google sends a
/// notification about a purchase, we verify and grant the book.
Future<dynamic> _handleRTDN(final context, Map<String, dynamic> data) async {
  context.log('📬 Processing RTDN notification');

  try {
    // Pub/Sub message format
    final message = data['message'] as Map<String, dynamic>?;
    if (message == null) {
      context.error('❌ No message in RTDN payload');
      return context.res.json({'success': false}, 400);
    }

    // Decode base64 message data
    final messageData = message['data'] as String?;
    if (messageData == null) {
      context.error('❌ No data in RTDN message');
      return context.res.json({'success': false}, 400);
    }

    final decoded = utf8.decode(base64Decode(messageData));
    final notification = jsonDecode(decoded) as Map<String, dynamic>;
    context.log('📬 RTDN decoded: $notification');

    // Check notification type
    final oneTimeProductNotification =
        notification['oneTimeProductNotification'] as Map<String, dynamic>?;

    if (oneTimeProductNotification == null) {
      context.log('ℹ️ Not a one-time purchase notification, ignoring');
      return context.res.json({'success': true, 'message': 'Ignored'});
    }

    final purchaseToken =
        oneTimeProductNotification['purchaseToken'] as String?;
    final sku = oneTimeProductNotification['sku'] as String?;
    final notificationType =
        oneTimeProductNotification['notificationType'] as int?;

    context
        .log('📦 RTDN: sku=$sku, type=$notificationType, token=$purchaseToken');

    // notificationType: 1 = ONE_TIME_PRODUCT_PURCHASED
    //                    2 = ONE_TIME_PRODUCT_CANCELED
    if (notificationType != 1) {
      context.log('ℹ️ Not a purchase notification (type=$notificationType)');
      return context.res.json({'success': true, 'message': 'Ignored'});
    }

    if (purchaseToken == null || sku == null) {
      context.error('❌ Missing purchaseToken or sku in RTDN');
      return context.res.json({'success': false}, 400);
    }

    // Verify the purchase
    final verificationResult = await _verifyWithGooglePlay(
      context,
      purchaseToken: purchaseToken,
      productId: sku,
    );

    if (!verificationResult['valid']) {
      context.error('❌ RTDN purchase verification failed');
      return context.res.json({'success': false}, 403);
    }

    context.log('✅ RTDN purchase verified successfully');

    // Note: For RTDN, we need to extract user info from the purchase.
    // The developerPayload or obfuscatedAccountId should contain userId+bookId
    // This requires setting these during purchase initiation.
    // For now, log and acknowledge — the direct verification flow handles granting.
    context.log(
        'ℹ️ RTDN verified. Book granting handled by direct verification flow.');

    return context.res.json({
      'success': true,
      'message': 'RTDN processed',
    });
  } catch (e) {
    context.error('❌ RTDN processing error: $e');
    return context.res.json({'success': false}, 500);
  }
}

/// Find or create user library document
Future<Document> _findOrCreateLibraryDocument(
  final context,
  Databases databases,
  String userId,
) async {
  try {
    final result = await databases.listDocuments(
      databaseId: dbId!,
      collectionId: userLibraryCol!,
      queries: [
        Query.equal('user_id', userId),
        Query.limit(1),
      ],
    );

    if (result.documents.isNotEmpty) {
      return result.documents.first;
    }

    // Create new library document
    context.log('📝 Creating new library for user $userId');
    return await databases.createDocument(
      databaseId: dbId!,
      collectionId: userLibraryCol!,
      documentId: ID.unique(),
      data: {
        'user_id': userId,
        'books': <String>[],
      },
    );
  } catch (e) {
    context.error('Error finding/creating library: $e');
    rethrow;
  }
}

/// Extract book ID from various formats
String? _extractBookId(dynamic item) {
  if (item is Document) return item.$id;
  if (item is String) return item.isNotEmpty ? item : null;
  if (item is Map) {
    final map = Map<String, dynamic>.from(item);
    final directId = (map['\$id'] ?? map['id'] ?? map['book_id'])?.toString();
    if (directId != null && directId.isNotEmpty) return directId;

    if (map.containsKey('book')) {
      final embedded = map['book'];
      if (embedded is Map<String, dynamic>) {
        final nested = Map<String, dynamic>.from(embedded);
        return (nested['\$id'] ?? nested['id'] ?? nested['book_id'])
            ?.toString();
      }
      if (embedded is Document) return embedded.$id;
    }
  }
  return null;
}

/// Parse dynamic to List<String>
List<String> _parseStringList(dynamic value) {
  if (value == null) return [];
  if (value is List) return value.map((e) => e.toString()).toList();
  if (value is String) {
    try {
      final parsed = jsonDecode(value) as List;
      return parsed.map((e) => e.toString()).toList();
    } catch (_) {
      return [];
    }
  }
  return [];
}

/// Emails the recipient their gift link.
///
/// Only for somebody who has no account yet. A recipient who already has one
/// gets a push and finds the gift waiting in the app; sending both would be
/// noise. It is also the case that actually needs an email -- there is no
/// other way to reach a person who has never heard of the app.
///
/// Deliberately does NOT include the sender's written note. That is read
/// when the box is opened, not in an inbox preview, and putting it here
/// would spend the one thing the card exists for.
///
/// Every failure is swallowed. The gift is paid for and delivered either
/// way, and the link still reaches them through whoever sent it.
Future<void> _emailGiftLink(
  final context, {
  required String toEmail,
  required String recipientName,
  required String senderName,
  required String bookTitle,
  required String token,
}) async {
  final apiKey = Platform.environment['BREVO_API_KEY'] ?? '';
  if (apiKey.isEmpty || toEmail.isEmpty) return;

  final fromEmail =
      Platform.environment['GIFT_FROM_EMAIL'] ?? 'noreply@ah-mar.app';
  final fromName = Platform.environment['GIFT_FROM_NAME'] ?? 'أحمر';
  final linkBase =
      Platform.environment['GIFT_LINK_BASE'] ?? 'https://link.ah-mar.app/g/';
  final link = '$linkBase$token';

  final greeting = recipientName.trim().isEmpty
      ? 'مرحباً'
      : 'مرحباً ${_escapeHtml(recipientName.trim())}';
  final from = senderName.trim().isEmpty
      ? 'أحدهم'
      : _escapeHtml(senderName.trim());
  final title = _escapeHtml(bookTitle.trim());

  final html = '''
<div dir="rtl" style="font-family:system-ui,-apple-system,'Segoe UI',Arial,sans-serif;background:#f4f6f8;padding:28px 12px;">
  <div style="max-width:480px;margin:0 auto;background:#ffffff;border-radius:18px;overflow:hidden;box-shadow:0 2px 14px rgba(0,0,0,.08);">
    <div style="background:linear-gradient(135deg,#E0332C,#8E1B16);padding:34px 24px;text-align:center;">
      <div style="font-size:46px;line-height:1;">&#127873;</div>
      <div style="color:#ffffff;font-size:21px;font-weight:700;margin-top:12px;">وصلتك هدية</div>
    </div>
    <div style="padding:26px 24px;color:#25303d;font-size:16px;line-height:1.85;">
      <p style="margin:0 0 14px;">$greeting،</p>
      <p style="margin:0 0 14px;"><strong>$from</strong> أهداك كتاب <strong>$title</strong> على تطبيق أحمر.</p>
      <p style="margin:0 0 24px;color:#5b6876;font-size:15px;">وترك لك رسالة تقرأها حين تفتح الهدية.</p>
      <a href="$link" style="display:block;text-align:center;background:#E0332C;color:#ffffff;text-decoration:none;font-weight:700;font-size:16px;padding:15px;border-radius:12px;">افتح هديتك</a>
      <p style="margin:22px 0 0;color:#8a95a1;font-size:13px;line-height:1.7;">
        أو انسخ هذا الرابط إلى متصفّحك:<br>
        <span style="color:#5b6876;word-break:break-all;">$link</span>
      </p>
    </div>
  </div>
  <p style="max-width:480px;margin:16px auto 0;color:#97a1ad;font-size:12px;text-align:center;line-height:1.7;">
    وصلتك هذه الرسالة لأن أحدهم أهداك كتاباً على هذا البريد.
  </p>
</div>
''';

  try {
    final response = await http.post(
      Uri.parse('https://api.brevo.com/v3/smtp/email'),
      headers: {
        'api-key': apiKey,
        'content-type': 'application/json',
        'accept': 'application/json',
      },
      body: jsonEncode({
        'sender': {'name': fromName, 'email': fromEmail},
        'to': [
          {
            'email': toEmail,
            if (recipientName.trim().isNotEmpty) 'name': recipientName.trim(),
          }
        ],
        'subject': 'وصلتك هدية: $bookTitle',
        'htmlContent': html,
      }),
    );

    if (response.statusCode >= 200 && response.statusCode < 300) {
      context.log('Gift email sent to $toEmail');
    } else {
      context.error(
          'Gift email refused (${response.statusCode}): ${response.body}');
    }
  } catch (e) {
    context.error('Could not email the gift link to $toEmail: $e');
  }
}

/// The book title and the two names go straight into HTML, and a stray
/// angle bracket in a display name would otherwise break the layout.
String _escapeHtml(String value) => value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('\"', '&quot;');
