import com.roncoo.eshop.storm.http.HttpClientUtils;

/**
 * @author yangfan
 * @date 2018/03/04
 */
public class Test {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            String url = "http://192.168.2.203/product?productId=" + i + "&requestPath=product&shopId=1";
            HttpClientUtils.sendGetRequest(url);
        }

        for (int i = 0; i < 10; i++) {
            String url = "http://192.168.2.203/product?productId=1&requestPath=product&shopId=1";
            HttpClientUtils.sendGetRequest(url);
        }

//        int percent5 = 6- (int)Math.floor(6 * 0.95);
//        System.out.println(percent5);
    }
}
