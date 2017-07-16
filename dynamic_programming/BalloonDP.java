import java.util.HashMap;
import java.util.Map;


/**
 * https://leetcode.com/problems/burst-balloons/#/description 
 * 
 * Given n balloons, indexed from 0 to n-1. Each balloon is painted with a number on it represented by array nums. You are asked to burst all the balloons. If the you burst balloon i you will get nums[left] * nums[i] * nums[right] coins. Here left and right are adjacent indices of i. After the burst, the left and right then becomes adjacent.

	Find the maximum coins you can collect by bursting the balloons wisely.
	
	Note: 
	(1) You may imagine nums[-1] = nums[n] = 1. They are not real therefore you can not burst them.
	(2) 0 ≤ n ≤ 500, 0 ≤ nums[i] ≤ 100
	
	Example:
	
	Given [3, 1, 5, 8]
	
	Return 167
	
	    nums = [3,1,5,8] --> [3,5,8] -->   [3,8]   -->  [8]  --> []
	   coins =  3*1*5      +  3*5*8    +  1*3*8      + 1*8*1   = 167

 * @author parirajaram
 *
 */

public class BalloonDP {

	// this recurrence works

	public static int doit_memo(int[] nums, int start, int end, Map<String, Integer> memo) {
		int max = 0;
		int N = nums.length;
		int temp = 0;
		if (start == end) { // System.out.println(" " +
							// (nums[start-1]*nums[start]*nums[end+1]));
			return nums[start - 1] * nums[start] * nums[end + 1];
		}

		int i = 0;
		int j = 0;
		int left = 0;
		int right = 0;

		for (i = start; i <= end; i++) {
			if (memo.containsKey(start + ":" + (i - 1)))
				left = memo.get(start + ":" + (i - 1));
			else {
				left = doit_memo(nums, start, i - 1, memo);
				memo.put(start + ":" + (i - 1), left);
			}

			if (memo.containsKey((i + 1) + ":" + end))
				right = memo.get((i + 1) + ":" + end);
			else {
				right = doit_memo(nums, i + 1, end, memo);
				memo.put((i + 1) + ":" + end, right);
			}

			temp = left + nums[start - 1] * nums[i] * nums[end + 1] + right;
			// System.out.println(" temp " + temp + " " + nums[i]);
			if (temp > max) {
				/// System.out.println("max ");
				max = temp;

			}
		}
		// System.out.println("max th " + j + " " + nums[j]);
		return max;
	}

	public static int doit(int[] nums, int start, int end) {
		int max = 0;
		int N = nums.length;
		int temp = 0;
		if (start == end) { // System.out.println(" " +
							// (nums[start-1]*nums[start]*nums[end+1]));
			return nums[start - 1] * nums[start] * nums[end + 1];
		}

		int i = 0;
		int j = 0;
		for (i = start; i <= end; i++) {

			temp = doit(nums, start, i - 1) + nums[start - 1] * nums[i] * nums[end + 1] + doit(nums, i + 1, end);
			// System.out.println(" temp " + temp + " " + nums[i]);
			if (temp > max) {
				/// System.out.println("max ");
				max = temp;
				j = i;
			}
		}
		// System.out.println("max th " + j + " " + nums[j]);
		return max;
	}

	public static int maxCoins(int[] nums) {

		Map<String, Integer> memo = new HashMap();
		int[] nums2 = new int[nums.length + 2];
		nums2[0] = 1;
		nums2[nums2.length - 1] = 1;
		int i = 0;
		for (i = 1; i < nums2.length - 1; i++) {
			nums2[i] = nums[i - 1];

			// System.out.print (nums2[i]);
		}
		// System.out.print (nums2[i] + " " + nums2.length);
		return doit_memo(nums2, 1, nums2.length - 2, memo);
		// return doit(nums2, 1, nums2.length-2);

	}

	public static void main(String[] args) {

		int[] nums = { 3, 1, 5, 8 };

		System.out.println(maxCoins(nums));
	}

}
