import java.util.concurrent.Semaphore;
/**
 * 
 * @author parirajaram
 *
 * This code demonstrates a simple mechanism using binary semaphores to
 * schedule one thread to go after another one finishes. We loop three threads to 
 * go one after another for few times.
 */
public class GoOneAfterAnother {
	
	public static class Foo  {

		Semaphore sem1, sem2, sem3;
		 
		Foo() throws InterruptedException{
			  this.sem1 = new Semaphore(1);
			  this.sem2 = new Semaphore(1);
			  this.sem3 = new Semaphore(1);
			sem1.acquire();
			sem2.acquire();
		}
		
		void start1() throws InterruptedException{
		   sem3.acquire();
		   System.out.println("1");
		   sem1.release();
		}
		
		void start2() throws InterruptedException{
			sem1.acquire();
			
			System.out.println("2");
	
			sem2.release();
		}
		
		void start3() throws InterruptedException{
	
			sem2.acquire();
			System.out.println("3");
		
			sem3.release();
		}
	}
	
	static class F1 implements Runnable {
		Foo f;

		F1(Foo f){
			this.f=f;
		}
		@Override
		public void run() {
			 
			try {
				
				f.start1();
				f.start1();
				f.start1();
				
			} catch (InterruptedException e) {
				 
				e.printStackTrace();
			}
			 
		}
		
	}
	
	static class F2 implements Runnable {
		Foo f;

		F2(Foo f){
			this.f=f;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				f.start2();
				f.start2();
				f.start2();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	static class F3 implements Runnable {
		Foo f;

		F3(Foo f){
			this.f=f;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				f.start3();
				f.start3();
				f.start3();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public static void main(String[] args) throws InterruptedException {
		
		Foo f = new Foo();
		F1 f1 = new F1(f);
		F2 f2 = new F2(f);
		F3 f3 = new F3(f);
		
		Thread t1 = new Thread(f1);
		Thread t2 = new Thread(f2);
		Thread t3 = new Thread(f3);
		
		t1.start();
		t2.start();
		t3.start();
		
		t1.join();
		t2.join();
		t3.join();
	}

}
