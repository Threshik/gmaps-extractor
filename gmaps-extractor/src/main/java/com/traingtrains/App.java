package com.traingtrains;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.github.cdimascio.dotenv.Dotenv;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class App {

    // Load environment variables
    private static final Dotenv dotenv = Dotenv.load();

    // MySQL DB
    private static final String DB_URL = dotenv.get("DB_URL");
    private static final String DB_USER = dotenv.get("DB_USER");
    private static final String DB_PASS = dotenv.get("DB_PASS");

    // Google Places API
    private static final String API_KEY = dotenv.get("GOOGLE_API_KEY");
    private static final String TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json";
    private static final String PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json";

    private static final OkHttpClient client = new OkHttpClient();

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter keyword (e.g., Restaurant, Hospital, IT Company): ");
        String keyword = sc.nextLine().trim();
        System.out.print("Enter location (e.g., New York, Chennai): ");
        String location = sc.nextLine().trim();
        sc.close();

        String query = keyword + " in " + location;
        String url = TEXT_SEARCH_URL + "?query=" + java.net.URLEncoder.encode(query, "UTF-8") + "&key=" + API_KEY;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            String sql = "INSERT INTO businesses (name, phone, email, website, address, pincode, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(sql);

            boolean moreResults = true;
            String nextPageToken = null;

            while (moreResults) {
                String requestUrl = url;
                if (nextPageToken != null) {
                    requestUrl += "&pagetoken=" + nextPageToken;
                    Thread.sleep(3000); // Required delay
                }

                Request request = new Request.Builder().url(requestUrl).build();
                Response response = client.newCall(request).execute();
                String json = response.body().string();
                JsonObject root = JsonParser.parseString(json).getAsJsonObject();

                JsonArray results = root.getAsJsonArray("results");
                System.out.println("Number of places: " + results.size());

                for (JsonElement e : results) {
                    JsonObject place = e.getAsJsonObject();
                    String name = place.has("name") ? place.get("name").getAsString() : "";
                    String address = place.has("formatted_address") ? place.get("formatted_address").getAsString() : "";
                    String pincode = extractPincode(address);
                    String phone = "";
                    String website = "";
                    String email = ""; // not provided
                    double lat = 0.0, lng = 0.0;

                    if (place.has("geometry")) {
                        JsonObject locationObj = place.getAsJsonObject("geometry").getAsJsonObject("location");
                        lat = locationObj.has("lat") ? locationObj.get("lat").getAsDouble() : 0.0;
                        lng = locationObj.has("lng") ? locationObj.get("lng").getAsDouble() : 0.0;
                    }

                    // Fetch phone & website from Details API
                    if (place.has("place_id")) {
                        String placeId = place.get("place_id").getAsString();
                        String detailsUrl = PLACE_DETAILS_URL + "?place_id=" + placeId
                                + "&fields=name,formatted_phone_number,website&key=" + API_KEY;

                        Request detailsRequest = new Request.Builder().url(detailsUrl).build();
                        Response detailsResp = client.newCall(detailsRequest).execute();
                        String detailsJson = detailsResp.body().string();
                        JsonObject detailsRoot = JsonParser.parseString(detailsJson).getAsJsonObject();

                        if (detailsRoot.has("result")) {
                            JsonObject result = detailsRoot.getAsJsonObject("result");
                            phone = result.has("formatted_phone_number")
                                    ? result.get("formatted_phone_number").getAsString()
                                    : "";
                            website = result.has("website") ? result.get("website").getAsString() : "";
                        }
                    }

                    // Check duplicates before insert
                    String checkSql = "SELECT COUNT(*) FROM businesses WHERE name=? AND address=?";
                    PreparedStatement checkStmt = conn.prepareStatement(checkSql);
                    checkStmt.setString(1, name);
                    checkStmt.setString(2, address);
                    ResultSet rs = checkStmt.executeQuery();
                    rs.next();
                    if (rs.getInt(1) == 0 && !name.isEmpty() && !address.isEmpty()) {
                        stmt.setString(1, name);
                        stmt.setString(2, phone);
                        stmt.setString(3, email);
                        stmt.setString(4, website);
                        stmt.setString(5, address);
                        stmt.setString(6, pincode);
                        stmt.setDouble(7, lat);
                        stmt.setDouble(8, lng);
                        stmt.executeUpdate();
                        System.out.println("Inserted: " + name);
                    }
                }

                nextPageToken = root.has("next_page_token") ? root.get("next_page_token").getAsString() : null;
                moreResults = nextPageToken != null;
            }
        }

        System.out.println("All data inserted successfully!");
    }

    private static String extractPincode(String address) {
        if (address == null)
            return "";
        Pattern p = Pattern.compile("\\b\\d{5,6}\\b");
        Matcher m = p.matcher(address);
        return m.find() ? m.group() : "";
    }
}
